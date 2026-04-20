'''
Grupo formado por:
- Lucía Martínez Miramontes (23C047)
- Laura García González (23C040)

Grafo de representación de la consulta:
Source -> Map(parse) -> Filter(speed > 90) -> Map(csv con campos seleccionados) -> Sink

Justificación:
- Source: lee el fichero CSV de entrada.
- Map(parse): transforma cada línea en una tupla tipada.
- Filter: selecciona únicamente los vehículos que superan 90 mph.
- Map(csv): adapta la salida al formato pedido.
- Sink: escribe la salida en formato CSV en /files/ejercicio1 con ficheros radar-XXXXX.csv.
'''

import argparse
from pyflink.common import Types
from pyflink.common.serialization import Encoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig

def parse_line(line):
    fields = line.strip().split(",")
    return (
        int(fields[0]),  
        int(fields[1]),  
        int(fields[2]),  
        int(fields[3]),  
        int(fields[4]),
        int(fields[5]),
        int(fields[6]),
        int(fields[7])
    )

def to_output_csv(event):
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

