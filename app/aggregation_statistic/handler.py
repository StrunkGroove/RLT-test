import json

from aiogram import Router, types
from pydantic import ValidationError as PydanticValidationError

from .services import AggregationOfStatisticalData
from .schemas import InputData

router = Router()


@router.message()
async def aggregation_of_statistical_data(message: types.Message) -> None:
    """
    This handler parses incoming JSON data from the message and uses it to perform aggregation of statistical data.
    
    :raises json.JSONDecodeError: if there's an error during JSON decoding.
    """
    try:
        message_data = json.loads(message.text)
    except json.JSONDecodeError:
        await message.answer("Ошибка при загрузке JSON-данных из сообщения")
        return 
        
    try:
        input_data = InputData(**message_data)
    except PydanticValidationError:
        await message.answer("Невалидный запос.")
        return 
        
    agg_data = AggregationOfStatisticalData()
    response = agg_data.main(input_data)

    await message.answer(response)
