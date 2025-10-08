from enum import Enum
from typing import Dict, Any, Callable, TypeVar, Generic
from pathlib import Path
import json

T = TypeVar('T')

class ApiType(Enum):
    DRINKING = "drinking"
    FOOD = "food" 
    RECIPE = "recipe"

class ApiBuilder:
    def __init__(self):
        self._config = {}
        self._callbacks = {}
    
    def with_url(self, url: str) -> 'ApiBuilder':
        self._config['url'] = url
        return self
    
    def with_key(self, key: str) -> 'ApiBuilder':
        self._config['api_key'] = key
        return self
    
    def with_pages(self, count: int) -> 'ApiBuilder':
        self._config['count_of_pages'] = count
        return self
    
    def with_type(self, api_type: ApiType) -> 'ApiBuilder':
        self._config['type_of_api'] = api_type
        return self
    
    def on_success(self, callback: Callable) -> 'ApiBuilder':
        self._callbacks['success'] = callback
        return self
    
    def on_failure(self, callback: Callable) -> 'ApiBuilder':
        self._callbacks['failure'] = callback
        return self
    
    def build(self) -> 'ApiCallBack':
        return ApiCallBack(self._config, self._callbacks)

class ApiCallBack(Generic[T]):
    __slots__ = ('_config', '_callbacks', '_data_cache')
    
    def __init__(self, config: Dict[str, Any], callbacks: Dict[str, Callable] = None):
        self._config = config
        self._callbacks = callbacks or {}
        self._data_cache = []
    
    @classmethod
    def from_config(cls, config: dict, callbacks: dict = None) -> 'ApiCallBack':
        return cls(config, callbacks)
    
    @classmethod
    def builder() -> ApiBuilder:
        return ApiBuilder()
    
    def __getitem__(self, key: str) -> Any:
        return self._config.get(key)
    
    def __setitem__(self, key: str, value: Any) -> None:
        self._config[key] = value
    
    def __call__(self, method_name: str, *args, **kwargs) -> Any:
        if method_name in self._callbacks:
            return self._callbacks[method_name](*args, **kwargs)
        return None
    
    def process_pages(self) -> None:
        pages = self._config.get('count_of_pages', 1)
        for page in range(1, pages + 1):
            try:
                data = self._fetch_page(page)
                self._data_cache.append(data)
                
                self._trigger('success', data)
            except Exception as e:
                self._trigger('failure', e)
                break
    
    def _fetch_page(self, page: int) -> T:
        raise NotImplementedError("Subclasses must implement _fetch_page")
    
    def _trigger(self, event: str, data: Any) -> None:
        if event in self._callbacks:
            self._callbacks[event](data)
    
    def save_cache(self, filepath: Path) -> None:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self._data_cache, f, indent=2, ensure_ascii=False)
    
    @property
    def api_key(self) -> str:
        return self._config.get('api_key', '')
    
    @api_key.setter
    def api_key(self, value: str) -> None:
        self._config['api_key'] = value
    
    @property
    def cached_data(self) -> list:
        return self._data_cache.copy()
        