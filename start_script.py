import importlib.machinery

script = importlib.machinery.SourceFileLoader('script','/Users/cofa/Projects/TestTask/script.py').load_module()
script.exec()