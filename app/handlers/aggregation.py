import json

from aiogram import Router, types
from .services import AggregationOfStatisticalData
from .schemas import InputData

router = Router()

@router.message()
async def aggregation_of_statistical_data(message: types.Message) -> None:
    """
    Handler will forward receive a message back to the sender

    By default, message handler will handle all message types (like a text, photo, sticker etc.)
    """

    agg_data = AggregationOfStatisticalData()
    message_data = json.loads(message.text)
    print(message_data)
    response = agg_data.main(InputData(**message_data))

    try:
        await message.answer(response)
    except TypeError:
        await message.answer("Nice try!")