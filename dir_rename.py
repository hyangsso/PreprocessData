import os

path = ''

for path, dir, files in os.walk(path):
    if len(dir) > 0:
        for dt in dir:
            try:
                float(dt)
            except ValueError:
                break
            else:
                if len(dt) == 8 and dt[2:4] == dt[4:6]:
                    rawpath = os.path.join(path,dt)
                    newpath = os.path.join(path,dt[:4]+dt[6:])
                    print(dt,'->',dt[:4]+dt[6:])
                    os.rename(rawpath,newpath)