def test_read_main(client):
    """
    Basic test to check if empty data api response as expected
    """
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == []
