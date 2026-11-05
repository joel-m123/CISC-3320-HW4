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
                print("Buffer full - waiting.")
                condition.wait()

            #Add an item to the buffer and 
            #notify the consumers that an item is available.
            buffer.append(item)
            print(f"Produced: {item}. Buffer size: {len(buffer)}")
            condition.notify_all()

#Basic consumer removing items from the buffer.
def consumer():
    for _ in range(10):
        with condition:
            #While buffer is empty, cosumer waits 
            #for the producer to produce an item. 
            while len(buffer) == 0:
                print("Buffer empty - waiting.")
                condition.wait()

            #Remove an item from the buffer and 
            #notify the producers that space is available.
            item = buffer.pop(0)
            print(f"Consumed: {item}. Buffer size: {len(buffer)}")
            condition.notify_all()


def main():
    #Create lists to hold producer and consumer threads.
    producers = []
    consumers = []

    num_prod_and_cons = int(input("Enter the number of producers and consumers: "))
    
    #Creating a number of consumer and producer
    #threads pointing to the respective functions.
    for i in range(num_prod_and_cons):
        producers.append(threading.Thread(target=producer))
        consumers.append(threading.Thread(target=consumer))

    # Start all threads
    for thread in producers + consumers:
        thread.start()

    # Wait for all threads to finish
    for thread in producers + consumers:
        thread.join()
    
    print("All producers finished, all consumers finished, 0 items left.")

if __name__ == "__main__":
    main()
