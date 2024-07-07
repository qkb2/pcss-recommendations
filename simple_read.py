# with open('dblp/dblp.xml') as f:
#     i = 0
#     x = f.readline()
#     while x is not None and i < 10000:
#         print(x)
#         i += 1
#         x = f.readline()
    
with open('dblp/omid.csv') as f:
    i = 0
    x = f.readline()
    while x is not None:
        i += 1
        x = f.readline()
        print(x)
    print(i)