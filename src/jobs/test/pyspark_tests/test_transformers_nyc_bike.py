
from ..jobs.handler_services.etl_services.transformers import RenameColumn,DropColumns
from ..jobs.handler_services.etl_services.interfaces import SinkData,DataTransformerObject,FactoryDataTransformer

from collections import namedtuple

NyBikeData=namedtuple("starttime","stoptime","start station latitude","start station longitude")

def test_RenameColumn(spark):
    
    input_data = [
        NyBikeData(
            '2023-01-01 00:00:00',
            '2023-01-01 00:00:00',
            40.73047309,
            -73.98672378
        )

    ]