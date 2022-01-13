from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict
from Facade import Facade


class Handler(ABC):

    @abstractmethod
    def set_next(self, handler: Handler) -> Handler:
        pass

    @abstractmethod
    def handle(self, request) -> Optional[str]:
        pass


class AbstractHandler(Handler):
    _next_handler: Handler = None
    facade: Facade = Facade()

    def set_next(self, handler: Handler) -> Handler:
        self._next_handler = handler
        return handler

    @abstractmethod
    def handle(self, request: Any) -> str:
        if self._next_handler:
            return self._next_handler.handle(request)

        return 'Nothing was implemented'


class PostHandler(AbstractHandler):
    def handle(self, request: Any) -> str:
        if request == "POST":
            self.facade.insert()
            return "Insert is successful"
        else:
            return super().handle(request)


class GetHandler(AbstractHandler):
    def handle(self, request: Any) -> Dict['str', Any]:
        if request == "GET":
            return {"tickets": self.facade.get_prod()}
        else:
            return super().handle(request)


class DeleteHandler(AbstractHandler):
    def handle(self, request: Any) -> str:
        if request == "DELETE":
            self.facade.delete()
            return "Delete is successful"
        else:
            return super().handle(request)


class PutHandler(AbstractHandler):
    def handle(self, request: Any) -> str:
        if request == "PUT":
            self.facade.update()
            return "Put is successful"
        else:
            return super().handle(request)