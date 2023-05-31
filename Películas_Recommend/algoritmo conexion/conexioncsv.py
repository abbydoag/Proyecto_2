from neo4j import GraphDatabase
from tkinter import *
import csv


class app(Tk):
    def __init__(self):
        Tk.__init__(self)
        self.title("Data Graphic")
        self.geometry("250x250")
        
    
        self.btn1 = Button(text="Exportar grafo",command=self.createUser, width=20)
        self.btn1.place(x=50,y=10)
        self.btn2 = Button(text="Importar data a NEO4J",command=self.IMPORTAR, width=20)
        self.btn2.place(x=50,y=40)
        self.BD = Neo4JConexion("neo4j+s://9d0eb28d.databases.neo4j.io", "neo4j", "")

    # FUNCIONES PRINCIPALES EN EL TKINTER
    def IMPORTAR(self):
        self.BD.exportoneo4j()

    # Validación Sesión con el USER
    def showUsers(self):
       self.BD.print_Users()

    def createUser(self):
        self.BD.importarcsv()



class Neo4JConexion:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_Users(self):
        with self.driver.session() as session:
            greeting = session.execute_write(self.showUsers)
            print(greeting)
    
    def print_Create(self, name):
         with self.driver.session() as session:
            greeting = session.execute_write(self.createUsers,name)
            print(greeting)
        

    def exportoneo4j(self): # módulo r (read)
        with open("", mode="r") as file:
            node_reader = csv.DictReader(file)
            nodes = list(node_reader)


        with open("", mode="r") as file:
            relationship_reader = csv.DictReader(file)
            relationships = list(relationship_reader)

        with self.driver.session() as session:
                query = "match (n) detach delete n"
                session.run(query)

        # Traducción - conversión para lectura de csv
        with self.driver.session() as session:
            for node in nodes:
                labels = node["labels"]
                labels = labels.replace("[","")
                labels = labels.replace("]","")
                labels = labels.replace("'","")
                properties = node["properties"]
                properties = properties.replace("{","")
                properties = properties.replace("}","")
                proplist = properties.split(",")
                cont = 0
                newProperties = "{"
                for x in proplist:
                    proplist2 = x.split(":")
                    prop = proplist2[0]
                    prop = prop.replace("'","")
                    newProperties+=prop+":"+proplist2[1]
                    if(cont < (len(proplist)-1)):
                        newProperties+=", "
                    cont+=1
                newProperties+="}"

                query = "CREATE (n:{}) SET n = {}".format(labels, newProperties)
                print(query)
                session.run(query)

        with self.driver.session() as session:
            for relationship in relationships:
                start_node_id = relationship["start_node_id"]
                end_node_id = relationship["end_node_id"]
                relationship_type = relationship["type"]

                query = "".format(
                    start_node_id, end_node_id, relationship_type
                )
                print(query)
                session.run(query)
        
        print("Termino de ingresar datos")

    def importarcsv(self):
        with self.driver.session() as session:
    # Export nodes
            nodes = session.run("MATCH (n) RETURN n").value()
            with open("nodes.csv", mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["node_id", "labels", "properties"])
                for node in nodes:
                    writer.writerow([node.id, list(node.labels), dict(node.items())])

            # Export relationships
            relationships = session.run("MATCH ()-[r]->() RETURN r").value()
            with open("name", mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["relationship_id", "start_node_id", "end_node_id", "type", "properties"])
                for relationship in relationships:
                    writer.writerow([relationship.id, relationship.start_node.id, relationship.end_node.id, relationship.type, dict(relationship.items())])



bucle = app()
bucle.mainloop()




