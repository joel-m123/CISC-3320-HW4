import threading

#Bounded buffer to store data.
buffer = []
BUFFER_CAPACITY = 10

#Lock + wait condition to synchronize 
#producer and consumer threads.
condition = threading.Condition() 

#Basic producer adding items to the buffer with condtions.
def producer():
    for item in range(10):
        #Acquire a lock to ensure that only one 
        #thread can access the buffer at a time.
        with condition:
            #Check if the buffer is full, 
            #if so wait until the consumer consumes an item.
            while len(buffer) >= BUFFER_CAPACITY:
                print("Buffer is full, waiting.")
                condition.wait()

            #Add an item to the buffer and 
            #notify the consumer that an item is available.
            buffer.append(item)
            print(f"Produced: {item}")
            condition.notify()

#Basic consumer removing items from the buffer.
def consumer():
    for _ in range(10):
        with condition:
            #While buffer is empty, cosumer waits 
            #for the producer to produce an item. 
            while len(buffer) == 0:
                print("Buffer is empty, waiting.")
                condition.wait()

            #Remove an item from the buffer and 
            #notify the producer that space is available.
            item = buffer.pop(0)
            print(f"Consumed: {item}")
            condition.notify()


def main():
    #Creating a consumer and producer thread pointing
    #to the respective functions.
    producer_thread = threading.Thread(target=producer)
    consumer_thread = threading.Thread(target=consumer)

    #Start threads and executes the functions.
    producer_thread.start()
    consumer_thread.start()

    #Main() waits for function to finish executing
    #before continuing.
    producer_thread.join()
    consumer_thread.join()

    print("Exiting.")

if __name__ == "__main__":
    main()
