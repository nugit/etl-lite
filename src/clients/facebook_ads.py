
def extract_data_1(**context):
    print(context['templates_dict']['myarg1'])
    print(context['templates_dict']['myarg2'])
    print("called extract_data_1")


def extract_data_2(**context):
    print(context['templates_dict']['myarg1'])
    print(context['templates_dict']['myarg2'])
    print("called extract_data_2")
