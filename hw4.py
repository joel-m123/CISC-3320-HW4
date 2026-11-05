import threading

#Bounded buffer to store data.
buffer = []
buffer_capacity = 10

#Lock + wait condition to synchronize 
#producer and consumer threads.
condition = threading.Condition() 
ready = False

#Basic producer adding items to the buffer with condtions.
def producer():
    for item in range(10):
        #Acquire a lock to ensure that only one 
        #thread can access the buffer at a time.
        with condition:
            #Check if the buffer is full, 
            #if so wait until the consumer consumes an item.
            while len(buffer) >= buffer_capacity:
                print("Buffer is full, waiting.")
                condition.wait()

            #Add an item to the buffer and 
            #notify the consumer that an item is available.
            buffer.append(item)
            print(f"Produced: {item}")
            condition.notify()

#Basic consumer removing items from the buffer.
def consumer():
    while len(buffer) > 0:
        if len(buffer) > 0:
            item = buffer.pop(0)
            print(f'Consumed: {item}')
        else:
            print('Buffer is empty.')


def main():
    #Creating a consumer and producer thread pointing
    #to the respective functions.
    producer_thread = threading.Thread(target=producer)
    consumer_thread = threading.Thread(target=consumer)

    #Start threads and executes the functions.
    producer_thread.start()
    consumer_thread.start()

    #Main() waits for function to finsih executing
    #before continuing.
    producer_thread.join()
    consumer_thread.join()

    print("Exiting.")

if __name__ == "__main__":
    main()
