from os import link
from turtle import position


class Element (object):
    def __init__(self,value):
        self.value = value
        self.next = None
        
class LinkedList (object):
    def __init__(self, head = None):
        self.head = head
    
    def append(self,value):
        current = self.head
        if self.head:
            while current.next:
                current = current.next
            current.next = value
        else:
            self.head = value
    
    def get_position(self,position):
        counter = 1
        current = self.head
        if position < 1:
            return None
        while current and counter <= position:
            if counter == position:
                return current.value
            current = current.next
            counter += 1
        return None
    
    def insert(self,value,position):
            counter = 1
            current = self.head
            if position > 1:
                while current and counter < position:
                    if(counter == position - 1):
                        value.next = current.next
                        current.next = value
                    current = current.next
                    counter+=1
            elif position == 1:
                value.next = self.head
                self.head = value
    
    def delete(self,value):
        current = self.head
        if current:
            if(current.value == value):
                self.head = current.next
                current = None
                return

        while current is not None:
            if(current.value == value):
                break
            before = current
            current = current.next
        before.next = current.next
        current = None

    def display(self):
        current = self.head
        while current is not None:
            print(current.value, end = ' ')
            current = current.next

"""ll = LinkedList(Element(1))
ll.append(Element(2))
ll.append(Element(3))
ll.append(Element(4))

ll.insert(Element(5),2)

ll.display()

ll.delete(4)
print("\n")
ll.display()
print("\n")
print(ll.get_position(1))"""

ll = LinkedList()
while True:
    print("MAIN MENU\n 1. Append \n 2. Insert \n 3. Delete \n 4. Get Position \n 5. Quit \n")
    choice = int(input("Enter the choice - "))
    
    if(choice == 1):
        value = int(input("Enter the value to be appended\n"))
        e1 = Element(value)
        ll.append(e1)
        print("value appended\n")
    elif(choice == 2):
        value = int(input("Enter the value\n"))
        e1 = Element(value)
        pos = int(input("Enter the position\n"))
        ll.insert(e1,pos)
        print("value inserted\n")
    elif(choice == 3):
        value = int(input("Enter the value to be deleted\n"))
        ll.delete(value)
        print("value deleted\n")
    elif(choice == 4):
        value = int(input("Enter the value\n"))
        print(ll.get_position(value))
    elif(choice == 5):
        ll.display()
        exit()
    