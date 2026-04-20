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


from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True, help="Ruta del fichero de entrada")
args = parser.parse_args()
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
ds = env.read_text_file(args.input)


def parse(line):
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

parsed = ds.map(parse, output_type=Types.TUPLE([
    Types.INT(), Types.INT(), Types.INT(), Types.INT(),
    Types.INT(), Types.INT(), Types.INT(), Types.INT()
])).filter(lambda x: x is not None)


speeding = parsed.filter(lambda x: x[2] > 90)
result = speeding.map(
    lambda x: f"{x[0]},{x[1]},{x[3]},{x[2]}",
    output_type=Types.STRING()
)


sink = FileSink.for_row_format(
    "/files/ejercicio1",
    Encoder.simple_string_encoder()
).with_output_file_config(
    OutputFileConfig.builder()
    .with_part_prefix("radar")
    .with_part_suffix(".csv")
    .build()
).build()


result.sink_to(sink)
env.execute("Radar")




