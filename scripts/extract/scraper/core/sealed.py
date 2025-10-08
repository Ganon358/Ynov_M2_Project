from typing import List, Any

def sealed(slots: List[str] = None):
    slots = slots or []
    
    if "__dict__" in slots:
        raise ValueError("__dict__ in __slots__ defeats memory optimization")
    
    class SealedMeta(type):
        @classmethod
        def __prepare__(mcs, name, bases, **kwargs):
            for base in bases:
                if isinstance(base, mcs):
                    raise TypeError(f"Sealed class '{base.__name__}' cannot be inherited")
            
            namespace = {"__slots__": slots} if slots else {}
            return namespace
            
        def __call__(cls, *args, **kwargs):
            instance = super().__call__(*args, **kwargs)
            if hasattr(instance, '_freeze'):
                instance._freeze()
            return instance
    
    return SealedMeta


class ImmutableMixin:
    __slots__ = ('_frozen',)
    
    def _freeze(self):
        object.__setattr__(self, '_frozen', True)
    
    def __setattr__(self, name: str, value: Any) -> None:
        if hasattr(self, '_frozen') and self._frozen:
            raise AttributeError(f"Cannot modify frozen instance: {name}")
        super().__setattr__(name, value)