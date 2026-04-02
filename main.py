import asyncio
import websockets
import json
import random
import time
import os
from datetime import datetime


# Server Configuration
MAP_SIZE = 3000
INITIAL_FOOD_COUNT = 300
MIN_FOOD_SIZE = 5
MAX_FOOD_SIZE = 15
TICK_RATE = 30  # Server updates per second

# Back4App Configuration
PORT = int(os.environ.get('PORT', 8765))
HOST = os.environ.get('HOST', '0.0.0.0')
SERVER_URL = os.environ.get('SERVER_URL', f'ws://localhost:{PORT}')  # Set via Back4App environment variable


class GameServer:
    def __init__(self):
        self.clients = {}  # websocket -> player_id
        self.players = {}  # player_id -> player_state
        self.foods = []
        self.next_player_id = 1
        self.running = True
        self.generate_initial_food()
        print(f"[SERVER] Initialized with {len(self.foods)} food items")
    
    def generate_initial_food(self):
        for _ in range(INITIAL_FOOD_COUNT):
            self.foods.append(self.create_food())
    
    def create_food(self, is_shed=False, x=None, y=None):
        if x is None or y is None:
            x = random.randint(MAX_FOOD_SIZE, MAP_SIZE - MAX_FOOD_SIZE)
            y = random.randint(MAX_FOOD_SIZE, MAP_SIZE - MAX_FOOD_SIZE)
        
        if is_shed:
            return {
                'id': f'food_{time.time()}_{random.randint(0, 99999)}',
                'x': x,
                'y': y,
                'radius': 4,
                'value': 5,
                'color': [150, 150, 150]
            }
        else:
            radius = random.randint(MIN_FOOD_SIZE, MAX_FOOD_SIZE)
            return {
                'id': f'food_{time.time()}_{random.randint(0, 99999)}',
                'x': x,
                'y': y,
                'radius': radius,
                'value': radius * 2,
                'color': [random.randint(50, 255), random.randint(50, 255), random.randint(50, 255)]
            }
    
    def create_player(self, player_id, name, color):
        player = {
            'id': player_id,
            'name': name,
            'x': random.randint(200, MAP_SIZE - 200),
            'y': random.randint(200, MAP_SIZE - 200),
            'radius': 10,
            'score': 0,
            'length': 150,
            'positions': [],
            'color': color,
            'alive': True
        }
        print(f"[SERVER] Created player {player_id} ({name}) at ({player['x']}, {player['y']})")
        return player
    
    def check_collisions(self):
        """Check for player-to-player collisions"""
        players_list = list(self.players.values())
        
        for i, player in enumerate(players_list):
            if not player['alive']:
                continue
                
            # Check collision with other players' bodies
            for j, other in enumerate(players_list):
                if i == j or not other['alive']:
                    continue
                
                # Skip the head segment, check body segments
                if len(other['positions']) > 10:
                    for k, pos in enumerate(other['positions'][10:], start=10):
                        # Calculate distance
                        dx = player['x'] - pos[0]
                        dy = player['y'] - pos[1]
                        distance = (dx * dx + dy * dy) ** 0.5
                        
                        # Calculate body radius at this segment
                        taper_factor = 1 - (k / len(other['positions'])) * 0.7
                        segment_radius = max(1, other['radius'] * taper_factor)
                        
                        if distance < player['radius'] + segment_radius:
                            # Player died!
                            player['alive'] = False
                            print(f"[SERVER] Player {player['id']} ({player['name']}) died by hitting {other['name']}")
                            
                            # Spawn food where they died
                            food_count = int(player['length'] / 10)
                            for _ in range(min(food_count, 50)):
                                offset_x = random.randint(-100, 100)
                                offset_y = random.randint(-100, 100)
                                self.foods.append(self.create_food(
                                    is_shed=True, 
                                    x=player['x'] + offset_x, 
                                    y=player['y'] + offset_y
                                ))
                            break
                
                if not player['alive']:
                    break
    
    async def handle_client(self, websocket, path):
        player_id = None
        try:
            # Wait for initial connection message
            message = await websocket.recv()
            data = json.loads(message)
            print(f"[SERVER] Received join request: {data}")
            
            if data['type'] == 'join':
                player_id = self.next_player_id
                self.next_player_id += 1
                player_name = data.get('name', f'Player {player_id}')
                player_color = data.get('color', [0, 200, 0])
                
                player = self.create_player(player_id, player_name, player_color)
                self.players[player_id] = player
                self.clients[websocket] = player_id
                
                # Send player their ID and initial game state
                init_message = {
                    'type': 'init',
                    'player_id': player_id,
                    'map_size': MAP_SIZE,
                    'server_url': SERVER_URL
                }
                await websocket.send(json.dumps(init_message))
                print(f"[SERVER] Sent init to player {player_id}: {init_message}")
                print(f"[SERVER] Total players: {len(self.players)}")
                
                # Game loop for this client
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        await self.handle_message(websocket, player_id, data)
                    except json.JSONDecodeError:
                        print(f"[SERVER] Invalid JSON from player {player_id}")
                        continue
        
        except websockets.exceptions.ConnectionClosed:
            print(f"[SERVER] Connection closed for player {player_id}")
        except Exception as e:
            print(f"[SERVER] Error handling client {player_id}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Clean up on disconnect
            if player_id and player_id in self.players:
                print(f"[SERVER] Player {player_id} disconnected")
                del self.players[player_id]
            if websocket in self.clients:
                del self.clients[websocket]
            print(f"[SERVER] Total players: {len(self.players)}")
    
    async def handle_message(self, websocket, player_id, data):
        if data['type'] == 'update':
            # Update player position and state
            player = self.players.get(player_id)
            if player:
                player['x'] = data['x']
                player['y'] = data['y']
                player['positions'] = data['positions']
                player['radius'] = data['radius']
                player['length'] = data['length']
                player['score'] = data['score']
                player['alive'] = data.get('alive', True)
                
                # Handle shed food
                if 'shed_food' in data and data['shed_food']:
                    shed = data['shed_food']
                    self.foods.append(self.create_food(True, shed['x'], shed['y']))
        
        elif data['type'] == 'eat':
            # Server validates eating
            food_id = data['food_id']
            self.foods = [f for f in self.foods if f['id'] != food_id]
            
            # Spawn new food to maintain count
            if len(self.foods) < INITIAL_FOOD_COUNT:
                self.foods.append(self.create_food())
        
        elif data['type'] == 'respawn':
            # Handle respawn
            player = self.players.get(player_id)
            if player:
                player_color = data.get('color', player['color'])
                new_player = self.create_player(player_id, player['name'], player_color)
                self.players[player_id] = new_player
                print(f"[SERVER] Player {player_id} ({player['name']}) respawned")
    
    async def broadcast_game_state(self):
        print("[SERVER] Starting broadcast loop")
        while self.running:
            if self.clients:
                # Check collisions
                self.check_collisions()
                
                # Prepare game state
                state = {
                    'type': 'state',
                    'players': list(self.players.values()),
                    'foods': self.foods,
                    'timestamp': time.time()
                }
                
                message = json.dumps(state)
                
                # Broadcast to all clients
                disconnected = []
                for websocket in list(self.clients.keys()):
                    try:
                        await websocket.send(message)
                    except Exception as e:
                        print(f"[SERVER] Error sending to client: {e}")
                        disconnected.append(websocket)
                
                # Clean up disconnected clients
                for ws in disconnected:
                    if ws in self.clients:
                        player_id = self.clients[ws]
                        if player_id in self.players:
                            del self.players[player_id]
                        del self.clients[ws]
                
                # Debug info every 5 seconds
                if int(time.time()) % 5 == 0:
                    print(f"[SERVER] Broadcasting to {len(self.clients)} clients, {len(self.players)} players, {len(self.foods)} food")
            
            await asyncio.sleep(1.0 / TICK_RATE)
    
    async def start_server(self):
        print("=" * 60)
        print("SLITHER.IO MULTIPLAYER SERVER")
        print("=" * 60)
        
        print(f"\n📡 SERVER CONFIGURATION")
        print(f"   Host: {HOST}")
        print(f"   Port: {PORT}")
        print(f"   WebSocket URL: {SERVER_URL}")
        print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        print("🎮 Server is running! Waiting for players...\n")
        
        # Start game state broadcast task
        broadcast_task = asyncio.create_task(self.broadcast_game_state())
        
        # Start WebSocket server
        try:
            async with websockets.serve(
                self.handle_client, 
                HOST, 
                PORT, 
                max_size=10**7, 
                ping_interval=20, 
                ping_timeout=10
            ):
                print(f"[SERVER] WebSocket server listening on {HOST}:{PORT}")
                await asyncio.Future()
        except KeyboardInterrupt:
            print("\n\n👋 Shutting down server...")
            self.running = False
            print("✅ Server stopped successfully!")

if __name__ == "__main__":
    print("\n🎮 Starting Slither.io Multiplayer Server...\n")
    server = GameServer()
    try:
        asyncio.run(server.start_server())
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")

