__author__ = 'return'


class extendabletype(type):
	def __new__(cls, name, bases, dict):
		if name == '__extend__':
			for cls in bases:
				for key, value in dict.items():
					if key == '__module__':
						continue
					setattr(cls, key, value)
			return None
		else:
			return super(extendabletype, cls).__new__(cls, name, bases, dict)
