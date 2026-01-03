# def Final(slots=[]):
#     if "__dict__" in slots:
#         raise ValueError("Having __dict__ in __slots__ breaks the purpose")
#     class _Final(type):
#         @classmethod
#         def __prepare__(mcs, name, bases, **kwargs):   
#             for b in bases:
#                 if isinstance(b, mcs):
#                     msg = "type '{0}' is not an acceptable base type"
#                     raise TypeError(msg.format(b.__name__))

#             namespace = {"__slots__":slots}
#             return namespace
#     return _Final

# class Foo(metaclass=Final(slots=["_z"])):
#     y = 1    
#     def __init__(self, z=1):       
#         self.z = 1

#     @property
#     def z(self):
#         return self._z

#     @z.setter
#     def z(self, val:int):
#         if not isinstance(val, int):
#             raise TypeError("Value must be an integer")
#         else:
#             self._z = val                

#     def foo(self):
#         print("I am sealed against monkey patching")

# https://stackoverflow.com/questions/16564198/pythons-equivalent-of-nets-sealed-class
# why use selead class by microsoft guy topic : https://stackoverflow.com/questions/7777611/when-and-why-would-you-seal-a-class

### calcule pas c'est comment faire une class selead en python un peut complex pour rien, mais haut gain de perf
# Ah et interdit de coder dans le init seul moi le peut attention je surveille !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!