from dataclasses import dataclass, field
from typing import Optional, Dict, List, Union
from decimal import Decimal
from core.sealed import sealed, ImmutableMixin


@dataclass(frozen=True, slots=True)
class NutrimentData:
    protides: Optional[float] = None
    glucides: Optional[float] = None
    lipides: Optional[float] = None
    calories: Optional[float] = None
    
    def __post_init__(self):
        for attr_name in ('protides', 'glucides', 'lipides', 'calories'):
            value = getattr(self, attr_name)
            if value is not None and (value < 0 or value > 10000):
                raise ValueError(f"Invalid {attr_name} value: {value}")


class ProductBuilder:
    __slots__ = ('_data', '_validators')
    
    def __init__(self):
        self._data = {}
        self._validators = {
            'code_barres': self._validate_barcode,
            'prix': self._validate_price
        }
    
    def __call__(self, **product_kwargs) -> 'ProductBuilder':
        for key, value in product_kwargs.items():
            if hasattr(self, key):
                getattr(self, key)(value)
            else:
                if key in self._validators:
                    self._validators[key](value)
                self._data[key] = value
        return self
    
    def nom(self, value: str) -> 'ProductBuilder':
        self._data['nom'] = value
        return self
    
    def marque(self, value: str) -> 'ProductBuilder':
        self._data['marque'] = value
        return self
    
    def code_barres(self, value: str) -> 'ProductBuilder':
        self._validate_barcode(value)
        self._data['code_barres'] = value
        return self
    
    def prix(self, value: Union[float, Decimal, str]) -> 'ProductBuilder':
        validated_price = self._validate_price(value)
        self._data['prix'] = validated_price
        return self
    
    def nutriments(self, data: Dict) -> 'ProductBuilder':
        self._data['nutriments'] = NutrimentData(**data) if isinstance(data, dict) else data
        return self
    
    def categories(self, value: List[str]) -> 'ProductBuilder':
        self._data['categories'] = list(value) if value else None
        return self
    
    def build(self) -> 'Product':
        return Product(**self._data)
    
    def _validate_barcode(self, value: str) -> str:
        if value and not value.isdigit():
            raise ValueError("Barcode must contain only digits")
        return value
    
    def _validate_price(self, value: Union[float, Decimal, str]) -> Optional[float]:
        if value is not None:
            try:
                price = float(value) if not isinstance(value, Decimal) else float(value)
                if price < 0:
                    raise ValueError("Price cannot be negative")
                return price
            except (ValueError, TypeError):
                raise ValueError(f"Invalid price format: {value}")
        return value


class Product(ImmutableMixin, metaclass=sealed([
    '_nom', '_nom_générique', '_marque', '_brands_tags', '_prix', '_image',
    '_score_nutritionnel', '_nutriments', '_poids_unité', '_emballage',
    '_categories', '_pays_origine', '_fabricant', '_pays_commercialisation',
    '_distributeur', '_code_barres', '_date_peremption', '_source', '_index'
])):
    
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if key == 'nutriments' and isinstance(value, dict):
                value = NutrimentData(**value)
            object.__setattr__(self, f'_{key}', value)
    
    @property
    def nom(self) -> Optional[str]:
        return getattr(self, '_nom', None)
    
    @property
    def nom_generique(self) -> Optional[str]:
        return getattr(self, '_nom_générique', None)
    
    @property
    def marque(self) -> Optional[str]:
        return getattr(self, '_marque', None)
    
    @property
    def brands_tags(self) -> Optional[List[str]]:
        return getattr(self, '_brands_tags', None)
    
    @property
    def prix(self) -> Optional[float]:
        return getattr(self, '_prix', None)
    
    @property
    def image(self) -> Optional[str]:
        return getattr(self, '_image', None)
    
    @property
    def score_nutritionnel(self) -> Optional[str]:
        return getattr(self, '_score_nutritionnel', None)
    
    @property
    def nutriments(self) -> Optional[NutrimentData]:
        return getattr(self, '_nutriments', None)
    
    @property
    def poids_unite(self) -> Optional[str]:
        return getattr(self, '_poids_unité', None)
    
    @property
    def emballage(self) -> Optional[str]:
        return getattr(self, '_emballage', None)
    
    @property
    def categories(self) -> Optional[List[str]]:
        return getattr(self, '_categories', None)
    
    @property
    def pays_origine(self) -> Optional[str]:
        return getattr(self, '_pays_origine', None)
    
    @property
    def fabricant(self) -> Optional[str]:
        return getattr(self, '_fabricant', None)
    
    @property
    def pays_commercialisation(self) -> Optional[str]:
        return getattr(self, '_pays_commercialisation', None)
    
    @property
    def distributeur(self) -> Optional[str]:
        return getattr(self, '_distributeur', None)
    
    @property
    def code_barres(self) -> Optional[str]:
        return getattr(self, '_code_barres', None)
    
    @property
    def date_peremption(self) -> Optional[str]:
        return getattr(self, '_date_peremption', None)
    
    @property
    def source(self) -> Optional[str]:
        return getattr(self, '_source', None)
    
    @property
    def index(self) -> Optional[str]:
        return getattr(self, '_index', None)
    
    def to_dict(self) -> Dict:
        result = {}
        for attr in self.__slots__:
            key = attr[1:]
            if key == 'nom_générique':
                key = 'nom_générique'
            elif key == 'poids_unité':
                key = 'poids_unité'
            
            value = getattr(self, attr, None)
            if isinstance(value, NutrimentData):
                result[key] = {
                    'protides': value.protides,
                    'glucides': value.glucides,
                    'lipides': value.lipides,
                    'calories': value.calories
                }
            else:
                result[key] = value
        return result
    
    def __repr__(self) -> str:
        return f"Product(nom='{self.nom}', code_barres='{self.code_barres}')"