"""
Grupo formado por:
- Nombre Apellido1 Apellido2 (num matrícula)
- Nombre Apellido1 Apellido2 (num matrícula)

Grafo de representación de la consulta:
Source -> Map(parse) -> AssignTimestampsAndWatermarks -> KeyBy(VID,XWay,Seg,Dir) -> Process(accidente) -> Sink

Justificación de la utilización de los operadores:
- Source: lee el fichero CSV de entrada.
- Map(parse): transforma cada línea en una tupla tipada.
- AssignTimestampsAndWatermarks: asigna el tiempo del evento como indica el enunciado.
- KeyBy(VID, XWay, Seg, Dir): agrupa por vehículo y contexto del tramo para analizar
  secuencias consecutivas dentro del mismo segmento y dirección.
- Process(accidente): mantiene los últimos 4 eventos consecutivos para detectar si el
  vehículo permanece detenido en la misma posición.
- Sink: escribe las alertas al fichero de salida.
"""

import argparse
from pyflink.common import Duration, Types
from pyflink.common.serialization import Encoder
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


def parse_line(line: str):
    parts = line.strip().split(",")
    return (
        int(parts[0]),  
        int(parts[1]),   
        int(parts[2]),  
        int(parts[3]),  
        int(parts[4]),  
        int(parts[5]),  
        int(parts[6]),  
        int(parts[7]),  
    )


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
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


class AccidentDetector(KeyedProcessFunction):
    """
    Clave: (VID, XWay, Seg, Dir)

    Guarda una ventana deslizante lógica de 4 eventos consecutivos del mismo vehículo
    en el mismo segmento/dirección. Si las 4 posiciones coinciden, emite una alerta.

    Como se avanza de 1 en 1, si hay más de 4 eventos consecutivos en la misma posición,
    se generan alertas solapadas, tal y como pide el enunciado.
    """

    def open(self, runtime_context: RuntimeContext):
        self.last_four = runtime_context.get_state(
            ValueStateDescriptor("last_four", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, value, ctx):
        window = self.last_four.value()
        if window is None:
            window = []

        window.append(value)
        if len(window) > 4:
            window = window[-4:]

        self.last_four.update(window)

        if len(window) == 4:
            pos0 = window[0][7]
            if all(event[7] == pos0 for event in window):
                time1 = window[0][0]
                time2 = window[3][0]
                vid = window[0][1]
                xway = window[0][3]
                seg = window[0][6]
                direction = window[0][5]
                pos = window[0][7]

                yield f"{time1},{time2},{vid},{xway},{seg},{direction},{pos}"


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
        .key_by(
            lambda x: (x[1], x[3], x[6], x[5]),
            key_type=Types.TUPLE([Types.INT(), Types.INT(), Types.INT(), Types.INT()])
        )
        .process(AccidentDetector(), output_type=Types.STRING())
    )

    sink = build_sink("/files/ejercicio3", "accidente")
    alerts.sink_to(sink)

    env.execute("Ejercicio 3 - Accidente")


if __name__ == "__main__":
    main()