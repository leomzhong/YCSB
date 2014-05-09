fp = open("overall_throughput")
lines = fp.readlines()
fp.close()

url_header = "http://chart.apis.google.com/chart?"
size = "500x300"

x = []
y = []
ny = []
y_scale_factor = 100
x_scale_factor = 100
for l in lines:
    str_x = l.split("\t")[0].strip()
    xt = float(str_x)/x_scale_factor
    x.append(str(xt))
    str_y = l.split("\t")[1].strip()
    yt = float(str_y)/y_scale_factor
    y.append(str(yt))

ny = map(float, y)
nx = map(float, x)
x_axis = ",".join(x)
y_axis = ",".join(y)
x_lables = "|".join(x)
y_lables = "|".join(y)
_min = str(min(y))
_max = str(max(y))
_avg = str(sum(ny)/float(len(ny)))
_min_x = str(min(x))
_max_x = str(max(nx))
#final_url = "%schs=%s&chd=t:%s&cht=lxy&chxt=x,y&chxl=0:|%s|1:|%s&chtt=throughput(y/100).vs.thread.per.machine(x/10)&chxt=x,y,r&chxl=2:|min|average|max&chxp=1,%s,%s,%s|0,%s&chxs=2,0000dd,13,-1,t,FF0000&chxtc=1,10|2,-500&chds=a&chxr=%s,%s"%(url_header,size,y_axis,x_lables, y_lables,_min, _max, _avg,x_axis, _min_x, _max_x)

final_url = "%schs=%s&cht=lxy&chd=t:%s|%s&chtt=throughput(y/100).vs.thread.per.machine(x/100)&chds=a&chxt=x,y"%(url_header,size,x_axis,y_axis)
print final_url
