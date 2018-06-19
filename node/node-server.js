var net = require('net');

var fs = require('fs');

const fileName = process.argv[2] || "read.txt"
console.log("File name is ", fileName)

const interval = parseInt(process.argv[3]) || 1000

console.log("Serving interval ", interval, " in ms" )

const linesPerTime = parseInt(process.argv[4]) || 1

console.log(`Shall stream ${linesPerTime} per send` )


if (fs.existsSync(fileName)) {
    console.log("Found file ", fileName)
    console.log("Shall serve the file once client connected..")
} else {
    console.log('\x1b[36m%s\x1b[0m', `File ${fileName} not found`);
    console.log('\x1b[36m%s\x1b[0m', "Shall exit the program");  //cyan
    process.exit(-1);
    return;
}

var array = fs.readFileSync(fileName).toString().split('\n')

const socketMap = {}

function feedData (socket) {
	let index = 1
	var timer = setInterval(() => {
		if (index >= array.length) {
			clearInterval(timer);
		} else {
		    for (let i = 0; i < linesPerTime; i++) {
		        if (socket in socketMap) {
                    socket.write(array[index] + '\r\n');
                    console.log("Feeding ",array[index])
                    index++;

                    if (index >= array.length) {
                        clearInterval(timer);
                        break;
                    }
                }
			}
		}
	}, interval);

	socketMap[socket] = timer
}

var server = net.createServer(function(socket) {

   socket.on('disconnect', function() {
      console.log('Got disconnect!');

        clearInterval(socketMap[socket])
        delete socketMap[socket]
   });


   socket.on('error', function() {
      console.log('Got disconnect!');

        clearInterval(socketMap[socket])
        delete socketMap[socket]
   });


	//socket.write('Echo server\r\n');
	socket.pipe(socket);
	feedData(socket);
});

const port = 55555
server.listen(port, '127.0.0.1');
console.log("Server running on ", port)