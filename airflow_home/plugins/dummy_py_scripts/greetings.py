def say_hi(name: str):
    print(f'Hi, {name}!')

def greet_with_fullname(task_instance):
    full_name = task_instance.xcom_pull(task_ids='3_concat_name')
    say_hi(name=full_name)