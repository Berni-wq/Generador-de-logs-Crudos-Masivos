import time

bloques = []
mb = 0
while True:
    bloques.append(bytearray(10*1024*1024))
    mb += 10
    print(f"Reservados: {mb} MB", flush=True)
    time.sleep(0.2)
