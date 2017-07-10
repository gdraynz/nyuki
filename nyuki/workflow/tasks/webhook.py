import time
import asyncio
import logging
from uuid import uuid4
from aiohttp import ClientSession
from tukio.task import register
from tukio.task.holder import TaskHolder


log = logging.getLogger(__name__)


@register('webhook', 'execute')
class WebhookTask(TaskHolder):

    __slots__ = ('task', 'session', 'webhooks')

    SCHEMA = {
        'type': 'object',
        'required': ['webhooks'],
        'additionalProperties': False,
        'properties': {
            'webhooks': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'required': ['method', 'url'],
                    'additionalProperties': False,
                    'properties': {
                        'method': {'type': 'string', 'enum': ['POST', 'PATCH']},
                        'url': {'type': 'string', 'minLength': 1},
                        'parameters': {
                            'type': 'array',
                            'items': {
                                'oneOf': [
                                   {'$ref': '#/definitions/field'},
                                   {'$ref': '#/definitions/literal'},
                                ]
                            }
                        }
                    }
                }
            },
        },
        'definitions': {
            'field': {
                'type': 'object',
                'required': ['type', 'value'],
                'additionalProperties': False,
                'properties': {
                    'type': {'type': 'string', 'enum': ['field']},
                    'value': {'type': 'string', 'minLength': 1},
                }
            },
            'literal': {
                'type': 'object',
                'required': ['type', 'name', 'value'],
                'additionalProperties': False,
                'properties': {
                    'type': {'type': 'string', 'enum': ['literal']},
                    'name': {'type': 'string', 'minLength': 1},
                    'value': {},
                }
            }
        }
    }

    def __init__(self, config):
        super().__init__(config)
        self.task = None
        self.session = None
        self.webhooks = []

    def report(self):
        return {'webhooks': self.webhooks}

    async def call(self, hook):
        webhook = {
            'uid': str(uuid4())[:8],
            'method': hook['method'],
            'url': hook['url'],
            'status': None,
            'exec_time': None,
        }
        self.task.dispatch_progress(webhook)
        self.webhooks.append(webhook)

        exec_time = time.time()
        async with self.session.request(hook['method'], hook['url']) as response:
            log.info('%s: %s', hook['url'], response.status)
            webhook['status'] = response.status
        webhook['exec_time'] = time.time() - exec_time

        self.task.dispatch_progress({
            'uid': webhook['uid'],
            'status': webhook['status'],
            'exec_time': webhook['exec_time'],
        })

    async def execute(self, event):
        tasks = [
            asyncio.ensure_future(self.call(hook))
            for hook in self.config.get('webhooks', [])
        ]
        if not tasks:
            return event

        self.task = asyncio.Task.current_task()
        async with ClientSession() as self.session:
            await asyncio.wait(tasks)

        return event
