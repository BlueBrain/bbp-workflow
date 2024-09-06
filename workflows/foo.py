from luigi import Task, Parameter


class Bar(Task):
    static_param = Parameter()
    dynamic_param = Parameter()
    def run(self):
        print(f"\n>>> {self.static_param} {self.dynamic_param}! <<<\n", flush=True)
