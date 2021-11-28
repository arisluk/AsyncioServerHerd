import asyncio
import aiohttp
import json
import sys
import time
import re

apikey = "AIzaSyCjtyKtdqDwJgaCI0HBo909-Y7pJZZTqsE"
port_nums = {"Riley" : 15415, "Jaquez" : 15416, "Juzang" : 15417, "Campbell" : 15418, "Bernard" : 15419}
port_network = {
	"Riley" : ["Jaquez", "Juzang"],
	"Jaquez" : ["Riley", "Bernard"],
	"Juzang" : ["Campbell", "Riley", "Bernard"],
	"Campbell" : ["Juzang", "Bernard"],
	"Bernard" : ["Jaquez", "Juzang", "Campbell"]
}
client_list = {}

async def log(msg):
	logs.write(msg+"\n")

async def server_write(msg, writer):
	try:
		msg += "\n"
		data = msg.encode()
		writer.write(data)
		await writer.drain()
		writer.close()
		await writer.wait_closed()
	except:
		await log("Error: Failed to write to server")

async def new_msg(reader, writer):
	line = await reader.read()
	curr_time = time.time()
	msg = line.decode()
	await log("INPUT: " + msg)
	msg_list = msg.split()
	msg = " ".join(msg_list)
	await parse_msg(msg, msg_list, writer, curr_time)

async def bad_cmd(msg, writer):
	await server_write("? " + msg, writer)
	await log("Error: Invalid cmd - " + msg)

async def parse_msg(msg, msg_list, writer, curr_time):
	if msg_list[0] == "IAMAT":
		if len(msg_list) != 4:
			await bad_cmd(msg, writer)
			return
		else:
			await iamat(msg, msg_list, writer, curr_time)
	elif msg_list[0] == "WHATSAT":
		if len(msg_list) != 4:
			await bad_cmd(msg, writer)
			return
		else:
			await whatsat(msg, msg_list, writer)
	elif msg_list[0] == "FLOOD":
		await flood(msg)
	else:
		await bad_cmd(msg, writer)
		return

async def flood(msg):
	msg_list = msg.split()
	client = msg_list[1]
	msg_time = msg_list[3]

	if (client in client_list) and (msg_time > client_list[client][1]):
		client_list[client] = msg_list[2:]
		asyncio.create_task(flood_subrt(msg))
	elif (client in client_list) and (msg_time <= client_list[client][1]):
		return
	else:
		client_list[client] = msg_list[2:]
		asyncio.create_task(flood_subrt(msg))

async def flood_subrt(msg):
	for serv in port_network[server_name]:
		port_num = port_nums[serv]
		try:
			reader, writer = await asyncio.open_connection("127.0.0.1", port_num)
			await log("Opened Connection to " + serv + " from " + server_name)
			await server_write(msg, writer)
		except:
			await log("Failed to open connection to " + serv + " from " + server_name)

async def iamat(msg, msg_list, writer, curr_time):
	client = msg_list[1]
	coord = msg_list[2]
	msg_time = msg_list[3]
	time_gap = curr_time - float(msg_time)

	at_msg = ""
	at_time = ""

	if time_gap > 0:
		at_time = "+" + str(time_gap)
	else:
		at_time = str(time_gap)

	at_msg = "AT " + server_name + " " + at_time + " " + (" ".join(msg_list[1:]))
	flood_msg = "FLOOD " + (" ".join(msg_list[1:])) + " " + server_name + " " + str(curr_time)
	await asyncio.create_task(flood(flood_msg))

	await log("OUTPUT: " + at_msg)
	await server_write(at_msg, writer)

async def process_coord(coord):
	if (coord[0] == "+") and (coord.find("-", 1) != -1):
		c_list = coord[1:].split("-")
		return [c_list[0], "-" + c_list[1]]
	elif (coord[0] == "-") and (coord.find("+", 1) != -1):
		return coord.split("+")
	elif coord[0] == "+":
		return coord[1:].split("+")
	elif coord[0] == "-":
		c_list = coord[1:].split("-")
		return ["-" + c_list[0], "-" + c_list[1]]
	else:
		return None


async def whatsat(msg, msg_list, writer):
	client = msg_list[1]
	radius = msg_list[2]
	num_places = int(msg_list[3])

	whatsat_msg = ""

	if client in client_list:
		info = client_list[client]
		coord = info[0]
		msg_time = info[1]
		server = info[2]
		curr_time = info[3]

		coord_arr = (await process_coord(coord))

		time_gap = float(curr_time) - float(msg_time)

		if time_gap > 0:
			at_time = "+" + str(time_gap)
		else:
			at_time = str(time_gap)

		whatsat_msg = "AT " + server + " " + at_time + " " + client + " " + (" ".join(info[0:2])) + "\n"

		places_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=" + str(coord_arr[0]) + "," + str(coord_arr[1]) + "&radius=" + radius + "&key=" + apikey

		async with aiohttp.ClientSession() as session:
			async with session.get(places_url) as response:
				html = await response.json()
				html['results'] = html['results'][:num_places]
				whatsat_msg = whatsat_msg + json.dumps(html, indent=3) + "\n"

		# from stackoverflow.com/questions/28901452/reduce-multiple-blank-lines-to-single-pythonically
		re.sub(r'\n\s*\n', '\n\n', whatsat_msg)

		await log("OUTPUT: " + whatsat_msg)
	else:
		whatsat_msg = "? " + msg
		await log("Invalid Client: ? " + whatsat_msg)

	await server_write(whatsat_msg, writer)

def main():
	if len(sys.argv) != 2:
		print("Invalid arguments")
		return

	global server_name
	server_name = sys.argv[1]

	if server_name not in port_nums:
		print("Invalid server name")
		return

	global event_loop
	global logs
	logs = open(server_name + ".txt", "w+")
	event_loop = asyncio.get_event_loop()
	socket_serv = asyncio.start_server(new_msg, "127.0.0.1", port_nums[server_name])
	server_loop = event_loop.run_until_complete(socket_serv)

	try:
		event_loop.run_forever()
	except KeyboardInterrupt:
		pass

	server_loop.close()
	event_loop.run_until_complete(server_loop.wait_closed())
	event_loop.close()
	logs.close()

if __name__ == "__main__":
    main()