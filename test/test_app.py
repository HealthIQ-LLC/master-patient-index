import json

from services.web.project.app import app
from services.web.project import timeit


@timeit
def test_index_route(client):
    response = client.get("/")
    assert json.loads(response.data.decode()) == {'hello': 'world'}
    assert app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] is False
    assert response.status_code == 200
