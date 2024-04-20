# Брокер сообщений для взаимодействия компонентов комплекса АТ-ТЕХНОЛОГИЯ

Данный проект предоставляет python-пакет для обеспечения взаимодействия различных компонентов комплекса АТ-ТЕХНОЛОГИЯ посредством сообщений через очереди [RabbitMQ](https://www.rabbitmq.com/)

## Установка

1. Установить python версии от 3.10
2. Установть пакет следующей командой:

```bash
pip install git+https://github.com/grigandal625/AT_QUEUE.git#egg=at-queue
```

или

```bash
pip install https://github.com/grigandal625/AT_QUEUE/raw/master/dist/at_queue-latest-py3-none-any.whl
```

Для систем типа `linux` может потребоваться напрямую указать версию python:

```bash
python3.10 -m pip install git+https://github.com/grigandal625/AT_QUEUE.git#egg=at-queue
```

или

```bash
python3.10 -m pip install https://github.com/grigandal625/AT_QUEUE/raw/master/dist/at_queue-latest-py3-none-any.whl
```

Также можно использовать пакетный менеджер pipenv

Далее для работы пакета необходимо установить и запустить брокер [RabbitMQ](https://www.rabbitmq.com/). Его можно установить и запустить в основной операционной системе, а также можно запустить с использованием `docker` с помощью команды 

```bash
docker run --rm -p 15672:15672 -p 5672:5672 rabbitmq:management
```

Либо воспользоваться онлайн-сервисами для разворачивания RabbitMQ в облаке (например, бесплатный сервис [CloudAMPQ](https://www.cloudamqp.com/))

Пакет установлен и готов к работе.

## Общая схема работы брокера

Работа брокера включает следующие функции

1. Регистрация компонента
2. Прием сообщений от компонента и их перенаправление указываемым компонентам

### Регистрация компонента

1. Запускается брокер и начинает функционировать в режиме ожидания сообщений
2. Запускается компонент и посылает сообщение о регистрации в зарезервированную очередь
3. Брокер принимает сообщение, сохраняет запись о решистрации в свой реестр, создает отдельную очередь для компонента, отправляет обратное сообщение компоненту с указанием идентификатора созданной очереди
4. Компонент принимает обратное сообщение от брокера, сохраняет идентификатор присланной очереди и все сообщения, связанные с этим компонентом будут теперь только в новой очереди

### Прием сообщений от компонента и их перенаправление указываемым компонентам

1. Запущен брокер и компоненты, все компоненты зарегистрированы, имеют свои очереди, записи о регистрации сохранены в реестре брокера
2. Компонент посылает сообщение брокеру с указанием имени второго компонента, которому необходимо переслать сообщение
3. Брокер принимает сообщение, по имени второго компонента в реестре ищет запись о регистрации второго компонента, получает идентификатор очереди второго компонента и перенаправляет сообщение в эту очередь
4. Второй компонент принимает сообщение из этой очереди и обрабатывает его

## Примеры

Здесь будет показан простейший пример использования брокера и компонентов
 
Пусть требуется создать два компонента.

- У первого компонента необходимо реализовать метод, принимающий два целых числа и возвращающего их сумму
- У второго компонента должен быть метод, запрашивающий из первого компонента сумму двух конкретных чисел

1. Пусть RabbitMQ развернут на `amqp://localhost:5672/`
2. Модуль брокера `broker.py`:

```python
from at_queue.core.session import ConnectionParameters
from at_queue.core.at_registry import ATRegistry
import asyncio


async def main():
    connection_parameters = ConnectionParameters('amqp://localhost:5672/') # Параметры подключения к RabbitMQ
    registry = ATRegistry(connection_parameters) # Создание брокера
    await registry.initialize() # Создание подключения брокера к RabbitMQ
    await registry.start() # Запуск брокера в режиме ожидания сообщений


if __name__ == '__main__':
    asyncio.run(main())
```

3. Модуль первого компонента 'int_summator.py':

```python
from at_queue.core.at_component import ATComponent
from at_queue.core.session import ConnectionParameters
from at_queue.utils.decorators import component_method
import asyncio
import logging

# Реализация самого компонента и метода сложения
class IntSummator(ATComponent):

    # Если указать подсказки типов для аргументов a: int и b: int, то входные данные будут провалидированы компонентом на соответствие типам
    @component_method
    def int_sum(self, a: int, b: int) -> int: 
        return a + b


async def main():

    connection_parameters = ConnectionParameters('amqp://localhost:5672/') # Параметры подключения к RabbitMQ
    int_summator = IntSummator(connection_parameters=connection_parameters) # Создание компонента
    await int_summator.initialize() # Подключение компонента к RabbitMQ
    await int_summator.register() # Отправка сообщения на регистрацию в брокер
    await int_summator.start() # Запуск компонента в режиме ожидания сообщений

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
```

4. Модуль второго компонента `sum_requester.py`:

```python
from at_queue.core.at_component import ATComponent
from at_queue.core.session import ConnectionParameters
from at_queue.utils.decorators import component_method
import asyncio

class SummRequester(ATComponent):

    # вызов внешнего метода с корректными данными
    @component_method
    async def request_sum(self) -> int:
        try:
            # вызов IntSummator.int_sum(a=5, b=18)
            result = await self.exec_external_method(
                reciever='IntSummator', 
                methode_name='int_sum', 
                method_args={'a': 5, 'b': 18}
            )
            return result
        except Exception as e:
            print(str(e))

    # вызов внешнего метода с некорректными данными
    @component_method
    async def request_sum_with_errors(self) -> int:
        try:
            # вызов IntSummator.int_sum(a=5, b='b') - b с типом, отличащимся от int
            result = await self.exec_external_method(
                reciever='IntSummator', 
                methode_name='int_sum', 
                method_args={'a': 5, 'b': 'b'}
            )
            return result
        except Exception as e:
            print(str(e))


async def main():
    connection_parameters = ConnectionParameters('amqp://localhost:5672/') # Параметры подключения к RabbitMQ

    summ_requester = SummRequester(connection_parameters=connection_parameters) # Создание компонента
    await summ_requester.initialize() # Подключение компонента к RabbitMQ
    await summ_requester.register() # Отправка сообщения на регистрацию в брокер

    # Запуск в режиме ожидания сообщений, не блокируя выполнение
    loop = asyncio.get_event_loop()
    task = loop.create_task(summ_requester.start())

    # Запрос суммы без ошибок
    res = await summ_requester.request_sum()
    print('External result:', res)

    # Запрос суммы с ошибками
    await summ_requester.request_sum_with_errors()

    # Обждание завершения
    await task


if __name__ == '__main__':
    asyncio.run(main())
```