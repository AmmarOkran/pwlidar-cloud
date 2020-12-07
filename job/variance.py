"""

AUTHOR: Ammar Okran

DATE: 12/10/2020

"""

import numpy as np

def variancex_y(inFile):
    
    X = np.sort(inFile.x)
    Y = np.sort(inFile.y)
#     Additional information
    variancex = []
    Variancey = []
    s = 0
    ss = 0
    for i in range(len(inFile.x)):
        variancex.append(X[i] - s)
        Variancey.append(Y[i] - ss)
        s = X[i]
        ss = Y[i]
    variancex.remove(variancex[0])
    Variancey.remove(Variancey[0])
    deltax = round(max(variancex), 2)
    deltay = round(max(Variancey), 2)
#     print(variancex[:5])
#     print(deltax, deltay)
    delta = max(deltax, deltay)
    return delta