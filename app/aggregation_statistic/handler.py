import json

from aiogram import Router, types

from .services import AggregationOfStatisticalData
from .schemas import InputData

router = Router()


@router.message()
async def aggregation_of_statistical_data(message: types.Message) -> None:
    """
    Handles incoming messages by forwarding them back to the sender.

    This handler parses incoming JSON data from the message and uses it to perform aggregation of statistical data.
    :raises json.JSONDecodeError: if there's an error during JSON decoding.
    """

    agg_data = AggregationOfStatisticalData()

    try:
        message_data = json.loads(message.text)
    except json.JSONDecodeError:
        await message.answer("Ошибка при загрузке JSON-данных из сообщения")

    response = agg_data.main(InputData(**message_data))

    await message.answer(response)
