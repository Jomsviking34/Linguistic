import pymongo
import webbrowser

client = pymongo.MongoClient('localhost', 27017)
mydb = client["News"]
mycol = mydb.collection

Newss = []

tbl = "<tr><td>Id</td><td>Title</td><td>Content</td><td>Cmt</td><td>Time</td><td>Link</td><td>People</td><td>Places</td></tr>"
Newss.append(tbl)

for y in mycol.find():
    a = "<tr><td>%s</td>" % y['_id']
    Newss.append(a)
    b = "<td>%s</td>" % y['title']
    Newss.append(b)
    c = "<td>%s</td>" % y['text']
    Newss.append(c)
    d = "<td>%s</td>" % y['cmts']
    Newss.append(d)
    e = "<td>%s</td>" % y['date']
    Newss.append(e)
    f = "<td>%s</td>" % y['link']
    Newss.append(f)
    g = "<td>%s</td>" % y['people']
    Newss.append(g)
    h = "<td>%s</td></tr>" % y['places']
    Newss.append(h)

contents = '''<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<meta content="text/html charset=UTF-8"
http-equiv="content-type">
<title> WebListr</title>
</head>
<style>
h1 {color:black}

table {text-align:center,overflow-x:auto,margin:auto}

</style>
<body>
<h1>Список новостей</h1>

<table border = "1">
%s
</table>

</body>
</html>
''' % (Newss)

filename = 'weblist.html'


def main(contents, filename):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(contents)


main(contents, filename)
webbrowser.open(filename)
