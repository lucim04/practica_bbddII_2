"""
Grupo formado por:
- Nombre Apellido1 Apellido2 (num matrícula)
- Nombre Apellido1 Apellido2 (num matrícula)

Grafo de representación de la consulta:
Source -> Map(parse) -> Filter(spd > 90) -> Map(csv) -> Sink

Justificación de la utilización de los operadores:
- Source: lee el fichero CSV de entrada.
- Map(parse): transforma cada línea en una tupla tipada.
- Filter: selecciona únicamente los vehículos que superan 90 mph.
- Map(csv): adapta la salida al formato pedido.
- Sink: escribe el resultado en fichero.
"""

import argparse

from pyflink.common import Types
from pyflink.common.serialization import Encoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig


def parse_line(line: str):
    parts = line.strip().split(",")
    return (
        int(parts[0]),  # Time
        int(parts[1]),  # VID
        int(parts[2]),  # Spd
        int(parts[3]),  # XWay
        int(parts[4]),  # Lane
        int(parts[5]),  # Dir
        int(parts[6]),  # Seg
        int(parts[7]),  # Pos
    )


def to_output_csv(event):
    # Formato: Time, VID, XWay, Spd
    return f"{event[0]},{event[1]},{event[3]},{event[2]}"


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

    alerts = (
        parsed
        .filter(lambda x: x[2] > 90)
        .map(to_output_csv, output_type=Types.STRING())
    )

    sink = build_sink("/files/ejercicio1", "radar")
    alerts.sink_to(sink)

    env.execute("Ejercicio 1 - Radar")


if __name__ == "__main__":
    main()