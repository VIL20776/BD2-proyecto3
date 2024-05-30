import json
import random
from faker import Faker

faker = Faker()

# Generar 1000 registros ficticios
registros = {}
for i in range(5, 1000):
    estudiante = {
        "personal": {
            "nombre": {"ver_01": faker.first_name()},
            "apellido": {"ver_01": faker.last_name()},
            "fecha_de_nacimiento": {"ver_01": faker.date_of_birth().strftime("%d-%m-%Y")},
            "telefono": {"ver_01": faker.phone_number()},
            "correo": {"ver_01": faker.email()},
            "direccion": {"ver_01": faker.street_address()}
        },
        "academico": {
            "correo": {"ver_01": faker.email()},
            "promedio": {"ver_01": round(random.uniform(60, 100), 1)},
            "cursos_actuales": {"ver_01": random.randint(1, 10)}
        }
    }
    registros[str(i).zfill(4)] = estudiante

# Escribir los registros en el archivo estudiantes.hfile
with open("regions/estudiantes.hfile", 'w') as f:
    json.dump(registros, f, indent=4)
