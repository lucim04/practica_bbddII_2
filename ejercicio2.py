"""
Grupo formado por:
- Nombre Apellido1 Apellido2 (num matrícula)
- Nombre Apellido1 Apellido2 (num matrícula)

Grafo de representación de la consulta:
Source -> Map(parse) -> AssignTimestampsAndWatermarks -> KeyBy(VID,XWay,Dir) -> Process(tramo) -> Sink

Justificación de la utilización de los operadores:
- Source: lee el fichero CSV de entrada.
- Map(parse): transforma cada línea en una tupla tipada.
- AssignTimestampsAndWatermarks: asigna el tiempo del evento como indica el enunciado.
- KeyBy(VID, XWay, Dir): agrupa los eventos por vehículo, autopista y dirección.
- Process(tramo): mantiene estado por vehículo para detectar recorridos completos entre
  los segmentos 52 y 56 y calcular la velocidad media.
- Sink: escribe la salida al fichero requerido.
"""

import argparse

from pyflink.common import Duration, Types
from pyflink.common.serialization import Encoder
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


MPH_PER_METER_PER_SECOND = 2.2369362920544


def parse_line(line: str):
    parts = line.strip().split(",")
    return (
        int(parts[0]),  # Time (segundos)
        int(parts[1]),  # VID
        int(parts[2]),  # Spd
        int(parts[3]),  # XWay
        int(parts[4]),  # Lane
        int(parts[5]),  # Dir
        int(parts[6]),  # Seg
        int(parts[7]),  # Pos
    )


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        # Flink trabaja en milisegundos
        return value[0] * 1000


def build_sink(output_path: str, prefix: str):
    return (
        FileSink
        .for_row_format(output_path, Encoder.simple_string_encoder())
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix(prefix)
            .with_part_suffix(".csv")
            .build()
        )
        .build()
    )


class AverageSpeedDetector(KeyedProcessFunction):
    """
    Clave: (VID, XWay, Dir)

    Mantiene el estado de una posible travesía del tramo [52,56].
    - start_event: primer evento en el borde por el que se entra (52 o 56)
    - end_event: último evento en el borde opuesto
    - active: indica si el vehículo está actualmente dentro del tramo
    - last_timer: temporizador de inactividad para cerrar el recorrido si el flujo termina
      mientras el vehículo sigue dentro del tramo

    Esto intenta respetar la condición del enunciado de escoger, si hay varios informes
    en 52 o 56, aquellos que cubran mayor distancia: primer evento del borde de entrada
    y último evento del borde de salida. :contentReference[oaicite:2]{index=2}
    """

    def open(self, runtime_context: RuntimeContext):
        self.active = runtime_context.get_state(
            ValueStateDescriptor("active", Types.BOOLEAN())
        )
        self.start_seg = runtime_context.get_state(
            ValueStateDescriptor("start_seg", Types.INT())
        )
        self.start_event = runtime_context.get_state(
            ValueStateDescriptor("start_event", Types.PICKLED_BYTE_ARRAY())
        )
        self.end_event = runtime_context.get_state(
            ValueStateDescriptor("end_event", Types.PICKLED_BYTE_ARRAY())
        )
        self.last_timer = runtime_context.get_state(
            ValueStateDescriptor("last_timer", Types.LONG())
        )

    def _clear_state(self, ctx):
        timer_ts = self.last_timer.value()
        if timer_ts is not None:
            ctx.timer_service().delete_event_time_timer(timer_ts)

        self.active.clear()
        self.start_seg.clear()
        self.start_event.clear()
        self.end_event.clear()
        self.last_timer.clear()

    def _register_inactivity_timer(self, ctx, event_time_seconds: int):
        old_timer = self.last_timer.value()
        if old_timer is not None:
            ctx.timer_service().delete_event_time_timer(old_timer)

        # Cada vehículo emite cada 30s. Esperamos un poco más para cerrar la travesía.
        new_timer = event_time_seconds * 1000 + 31000
        ctx.timer_service().register_event_time_timer(new_timer)
        self.last_timer.update(new_timer)

    def _avg_speed_mph(self, start_event, end_event):
        time1 = start_event[0]
        time2 = end_event[0]
        pos1 = start_event[7]
        pos2 = end_event[7]

        dt = time2 - time1
        if dt <= 0:
            return None

        distance_meters = abs(pos2 - pos1)
        speed_mps = distance_meters / dt
        return speed_mps * MPH_PER_METER_PER_SECOND

    def _finalize_if_complete(self):
        start_event = self.start_event.value()
        end_event = self.end_event.value()

        if start_event is None or end_event is None:
            return []

        time1 = start_event[0]
        time2 = end_event[0]
        vid = start_event[1]
        xway = start_event[3]
        direction = start_event[5]

        avg_spd = self._avg_speed_mph(start_event, end_event)
        if avg_spd is None:
            return []

        if avg_spd > 60.0:
            output = f"{time1},{time2},{vid},{xway},{direction},{avg_spd:.2f}"
            return [output]

        return []

    def process_element(self, value, ctx):
        seg = value[6]
        inside = 52 <= seg <= 56

        active = self.active.value()
        if active is None:
            active = False

        if inside:
            if not active:
                self.active.update(True)
                self.start_seg.clear()
                self.start_event.clear()
                self.end_event.clear()

            # Actualizamos temporizador de inactividad
            self._register_inactivity_timer(ctx, value[0])

            start_seg = self.start_seg.value()
            start_event = self.start_event.value()

            if start_seg is None:
                # Solo podemos arrancar una travesía completa si el primer borde es 52 o 56
                if seg in (52, 56):
                    self.start_seg.update(seg)
                    self.start_event.update(value)
            else:
                opposite_seg = 56 if start_seg == 52 else 52

                # Guardamos el último evento del borde opuesto para maximizar distancia
                if seg == opposite_seg:
                    self.end_event.update(value)

        else:
            # Si estaba dentro del tramo y sale, cerramos y emitimos si procede
            if active:
                for out in self._finalize_if_complete():
                    yield out
                self._clear_state(ctx)

    def on_timer(self, timestamp, ctx):
        active = self.active.value()
        if active:
            for out in self._finalize_if_complete():
                yield out
            self._clear_state(ctx)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Ruta del fichero de entrada")
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    ds = env.read_text_file(args.input)

    parsed = ds.map(
        parse_line,
        output_type=Types.TUPLE([
            Types.INT(), Types.INT(), Types.INT(), Types.INT(),
            Types.INT(), Types.INT(), Types.INT(), Types.INT()
        ])
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(0))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    timed = parsed.assign_timestamps_and_watermarks(watermark_strategy)

    alerts = (
        timed
        .key_by(lambda x: (x[1], x[3], x[5]), key_type=Types.TUPLE([Types.INT(), Types.INT(), Types.INT()]))
        .process(AverageSpeedDetector(), output_type=Types.STRING())
    )

    sink = build_sink("/files/ejercicio2", "radarTramo")
    alerts.sink_to(sink)

    env.execute("Ejercicio 2 - Radar de tramo")


if __name__ == "__main__":
    main()