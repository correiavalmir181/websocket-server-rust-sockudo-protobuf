"""Verifica que a limpeza de uma sessão antiga não desregistra o novo admin.

Inicie o servidor local e execute: python3 bench/test_admin_reconnect.py
"""

import asyncio
import json

import websockets


async def main():
    url = "ws://127.0.0.1:8080"
    auth = json.dumps({"type": "admin_auth", "adminId": "reconnect-check", "password": "admin123"})
    async with websockets.connect(url) as old_admin:
        await old_admin.send(auth)
        assert json.loads(await asyncio.wait_for(old_admin.recv(), 3))["type"] == "admin_welcome"

        async with websockets.connect(url) as client:
            await client.send(json.dumps({
                "type": "identification", "data": "android", "adminId": "reconnect-check",
                "androidId": "reconnect-device", "wallpaper": "",
            }))
            client_id = json.loads(await asyncio.wait_for(client.recv(), 3))["clientId"]
            await asyncio.wait_for(old_admin.recv(), 3)  # client_connected

            async with websockets.connect(url) as new_admin:
                await new_admin.send(auth)
                assert json.loads(await asyncio.wait_for(new_admin.recv(), 3))["type"] == "admin_welcome"
                await old_admin.close()
                # Assegura que cleanup_admin terminou antes da resposta do cliente.
                await asyncio.sleep(0.1)
                await client.send('resposta apos reconexao')
                response = json.loads(await asyncio.wait_for(new_admin.recv(), 3))
                assert response["type"] == "client_message", response
                assert response["clientId"] == client_id, response
                assert response["message"] == 'resposta apos reconexao', response

    print("reconexão de admin: resposta encaminhada à sessão atual")


if __name__ == "__main__":
    asyncio.run(main())
