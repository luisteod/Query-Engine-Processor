import proto_sql

column = "building"

dicionario = dict(building="utfrp",room_number="2",capacity="oitenta")
for key in iter(dicionario):
    if column in dicionario:
        print(dicionario.get(column))  

