from abc import ABC, abstractmethod
from typing import Protocol, TypeVar, Generic, Iterator, Any, Optional, Callable
from enum import Enum

T = TypeVar('T')
R = TypeVar('R')

class ProcessorProtocol(Protocol[T, R]):
    def process(self, data: T) -> R: ...
    def validate(self, data: T) -> bool: ...

class ApiStatus(Enum):
    IDLE = "idle"
    PROCESSING = "processing" 
    SUCCESS = "success"
    ERROR = "error"

class BaseApiHandler(ABC, Generic[T, R]):
    __slots__ = ('_status', '_callbacks', '_processor')
    
    def __init__(self, processor: Optional[ProcessorProtocol[T, R]] = None):
        self._status = ApiStatus.IDLE
        self._callbacks = {}
        self._processor = processor
    
    @abstractmethod
    def fetch(self, *args, **kwargs) -> T: ...
    
    @abstractmethod
    def transform(self, data: T) -> R: ...
    
    def __call__(self, *args, **kwargs) -> R:
        self._status = ApiStatus.PROCESSING
        try:
            raw_data = self.fetch(*args, **kwargs)
            if self._processor and not self._processor.validate(raw_data):
                raise ValueError("Data validation failed")
            result = self.transform(raw_data)
            self._status = ApiStatus.SUCCESS
            self._trigger_callback('success', result)
            return result
        except Exception as e:
            self._status = ApiStatus.ERROR
            self._trigger_callback('error', e)
            raise
    
    def register_callback(self, event: str, callback: Callable) -> 'BaseApiHandler':
        self._callbacks[event] = callback
        return self
    
    def _trigger_callback(self, event: str, data: Any) -> None:
        if event in self._callbacks:
            self._callbacks[event](data)
    
    @property
    def status(self) -> ApiStatus:
        return self._status

class ConfigurableHandler(BaseApiHandler[T, R]):
    __slots__ = ('_config', '_endpoint', '_key')
    
    def __init__(self, config: dict, processor: Optional[ProcessorProtocol[T, R]] = None):
        super().__init__(processor)
        self._config = config
        self._endpoint = config.get('url', '')
        self._key = config.get('api_key', '')
    
    def __getitem__(self, key: str) -> Any:
        return self._config.get(key)
    
    def __setitem__(self, key: str, value: Any) -> None:
        self._config[key] = value
        if key == 'url':
            self._endpoint = value
        elif key == 'api_key':
            self._key = value

class IterableApiHandler(ConfigurableHandler[T, R]):
    __slots__ = ('_page_size', '_max_pages')
    
    def __init__(self, config: dict, processor: Optional[ProcessorProtocol[T, R]] = None):
        super().__init__(config, processor)
        self._page_size = config.get('page_size', 50)
        self._max_pages = config.get('max_pages', float('inf'))
    
    def __iter__(self) -> Iterator[R]:
        page = 1
        while page <= self._max_pages:
            try:
                data = self.fetch_page(page)
                if not data:
                    break
                yield self.transform(data)
                page += 1
            except StopIteration:
                break
    
    @abstractmethod
    def fetch_page(self, page: int) -> T: ...

class MyIterable(ABC):
    @abstractmethod
    def __iter__(self): pass
    
    def get_iterator(self): return self.__iter__()
    
    @classmethod
    def __subclasshook__(cls, C):
        return NotImplemented if cls is not MyIterable else any("__iter__" in B.__dict__ for B in C.__mro__)