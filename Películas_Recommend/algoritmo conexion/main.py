from flask import Flask, request, render_template
from neo4j import GraphDatabase

class Neo4JExample:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_Users(self,name):
        with self.driver.session() as session:
            entrada = session.execute_write(self.UsersData,name)
        return entrada
    
    def callCreateNewUser(self,nombre,contra):
        with self.driver.session() as session:
            entrada = session.execute_write(self.createNewUser,nombre,contra)
        return entrada
    
    @staticmethod
    def UsersData(tx,name):
        result = tx.run('match (u:user{name: "'+name+'"}) return u.user, n.contra')
        print(result)
        bandera = []
        for record in result:
            bandera.append(record["n.name"])
            bandera.append(record["n.contra"])
        if(len(bandera) == 0):
            bandera.append("No existe")
        return bandera

    @staticmethod
    def createNewUser(tx,nombre,contra, in_vistas):
        result = tx.run('create (n:User{name: "'+nombre+'", contra: "'+contra+'"})')

        for x in in_vistas:
            print(x)
            query = "match (n:User{ name: '"+nombre+"'}), (n:movie {name:'"+x+"'}) create p=()-[:IN_VISTAS]->()"
            tx.run(query)




app = Flask(__name__,template_folder= 'path de carpeta') 
BD = Neo4JExample("neo4j+s://9d0eb28d.databases.neo4j.io", "neo4j", "")




def inicio():
    
    return render_template('index.html')

@app.route('/entrada1', methods=['POST'])
def entrada1():
    return render_template('peliculas.html')

@app.route('/buscarRecomendacion', methods=['POST'])
def buscarRecomendacion():
    nombre = request.form['nombre']
    contrasena = request.form['contrasena']
    arrClases = BD.CallGetClases()
    
        
        
    render_template('recomendaciones.html',busqueda = True, nombre = nombre, contrasena = contrasena)
     
    

@app.route('/menu', methods=['POST'])
def menu():
    nombre = request.form['nombre']
    contrasena = request.form['contrasena']
    return render_template('peliculas.html', nombre = nombre, contrasena = contrasena)





@app.route('/form', methods=['POST'])
def Form():
    nombre = request.form['nombre']
    contrasena = request.form['contrasena']

    ret = BD.print_Users(nombre)
    
    if(ret[0] == "No existe"):

        return render_template('login.html', flagError = True,mensaje="Usuario Inexistente")
    else:
        if(ret[0] == nombre and contrasena == ret[1]):

            return render_template('peliculas.html', nombre=nombre, contrasena=contrasena)
        else:
            return render_template('login.html', flagError = True,mensaje="Error los datos no son correctos, inténtlo de nuevo")

    

if __name__ == '__main__':
    app.run(debug=True)

