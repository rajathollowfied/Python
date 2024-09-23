str = 'gojo is fucking crazy on god fucking gojo bro'
caplist =[]
listed = str.split()

def cappedwords():
    for i in listed:
        caplist.append(i.capitalize())

    print(" ".join(caplist))

def dupwords():
    for i in range(0,len(listed)):
        count = 1
        for j in range(i+1,len(listed)):
            if(listed[i] == listed[j]):
                count = count + 1
                listed[j] = 0
        if(count > 1 and listed[i] != 0):
            print(listed[i])

cappedwords()
dupwords()