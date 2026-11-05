import queue
import threading
import multiprocessing
import time

#Bounded buffer to store data.
buffer = []
BUFFER_CAPACITY = 10

#Lock + wait condition to synchronize 
#producer and consumer threads.
condition = threading.Condition() 

def multi_producer(process_buffer):
    #Basic producer adding items to the buffer.
    for item in range(10):
        try:
            process_buffer.put_nowait(item)
        except queue.Full:
            #Wait if the buffer is full, then add an item to the buffer.
            process_buffer.put(item)
            print("Producer resumed.")

        print(f"Produced item: {item}. Buffer size: {process_buffer.qsize()}")
    
def multi_consumer(process_buffer):
    while True:
        #Wait for an item to be available in the buffer, then remove it.
        item = process_buffer.get()

        if item is None:
            print("Received stop signal - exiting.")
            break

        print(f"Consumed item: {item}. Buffer size: {process_buffer.qsize()}")

#Basic producer adding items to the buffer with condtions.
def thread_producer():
    for item in range(10):
        #Acquire a lock to ensure that only one 
        #thread can access the buffer at a time.
        with condition:
            was_waiting = False

            #Check if the buffer is full, 
            #if so wait until the consumer consumes an item.
            while len(buffer) >= BUFFER_CAPACITY:
                print("Buffer full - waiting.")
                was_waiting = True
                condition.wait()
            
            if was_waiting:
                print("Producer resumed.")

            #Add an item to the buffer and 
            #notify the consumers that an item is available.
            buffer.append(item)
            print(f"Produced item: {item}. Buffer size: {len(buffer)}")
            condition.notify_all()

#Basic consumer removing items from the buffer.
def thread_consumer():
    while True:
        with condition:
            #While buffer is empty, cosumer waits 
            #for the producer to produce an item. 
            while len(buffer) == 0:
                print("Buffer empty - waiting.")
                condition.wait()

            #Remove an item from the buffer and 
            #notify the producers that space is available.
            item = buffer.pop(0)
            #Check for sentinel value to stop the consumer thread.
            if item is None:
                print("Received stop signal - exiting.")
            else:
                print(f"Consumed item: {item}. Buffer size: {len(buffer)}")
            
            condition.notify_all()

        if item is None:
            break

def run_threading():
    num_prod_and_cons = int(input("(THREADING)Enter the number of producers and consumers: "))

    start_time = time.perf_counter()

    #Create lists to hold producer and consumer threads.
    thread_producers = []
    thread_consumers = []

    #Creating a number of consumer and producer
    #threads pointing to the respective functions.
    for i in range(num_prod_and_cons):
        thread_producers.append(threading.Thread(target=thread_producer))
        thread_consumers.append(threading.Thread(target=thread_consumer))

    #Start all threads
    for thread in thread_producers + thread_consumers:
        thread.start()

    #Wait for all producer threads to finish
    for thread in thread_producers:
        thread.join()

    #Send one stop signal per consumer
    for thread in thread_consumers:
        with condition:
            while len(buffer) >= BUFFER_CAPACITY:
                condition.wait()

            buffer.append(None)
            condition.notify_all()

    for thread in thread_consumers:
        thread.join()

    execution_time = time.perf_counter() - start_time
    
    print(f"All producers finished, all consumers finished, {len(buffer)} items left.")
    print(f"Threading execution time: {execution_time:.4f} seconds")

def run_multiprocessing():
    num_prod_and_cons = int(input("(MULTIPROCESSING) Enter the number of producers and consumers: "))

    start_time = time.perf_counter()

    #Create lists for multiprocessing workers
    multi_producers = []
    multi_consumers = []

    #Multiprocessing queue to hold data for processing.
    process_buffer = multiprocessing.Queue(maxsize=BUFFER_CAPACITY)

    #Creating a number of consumer and producers
    for i in range(num_prod_and_cons):
        multi_producers.append(multiprocessing.Process(target=multi_producer, args=(process_buffer,)))
        multi_consumers.append(multiprocessing.Process(target=multi_consumer, args=(process_buffer,)))

    for process in multi_producers + multi_consumers:
        process.start()

    for process in multi_producers:
        process.join()

    #Send one stop signal per consumer
    for process in multi_consumers:
        process_buffer.put(None)

    for process in multi_consumers:
        process.join()

    execution_time = time.perf_counter() - start_time

    print("All multiprocessing workers finished.")
    print(f"Multiprocessing execution time: {execution_time:.4f} seconds")


def main():
    run_threading()
    run_multiprocessing()


if __name__ == "__main__":
    main()
