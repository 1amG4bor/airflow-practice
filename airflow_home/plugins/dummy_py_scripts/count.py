from time import sleep

def count_back(n: int, steps__in_ms: int = 100):
    for i in range(n, -1, -1):
        sleep(steps__in_ms / 1000)
        print(f'Count: {i}')
