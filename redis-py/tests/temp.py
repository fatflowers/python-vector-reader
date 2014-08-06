__author__ = 'simon'


class a:
    def __init__(self):
        self.it = 1
    class b:
        def __init__(self):
            print('asdf')

            print(a.it)


p = a()
c = p.b()