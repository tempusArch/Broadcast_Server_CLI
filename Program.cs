using System.Net;
using System.Net.Sockets;
using System.Text;

namespace BroadcastServer;

class Program {
    private static List<TcpClient> clientRisuto = new List<TcpClient>();
    private const int port = 5035;
    private static TcpListener listener;
    private static bool isRunning = true;

    static async Task Main(string[] args) {
        listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        Console.WriteLine($"Server started on port {port}");

        _ = Task.Run(() => isShuttingDown());

        while (isRunning) {
            try {
                TcpClient client = await listener.AcceptTcpClientAsync();
                clientRisuto.Add(client);
                Console.WriteLine("Client connected");

                _ = Task.Run(() => ProcessMessage(client));

            } catch (SocketException ex) when (ex.SocketErrorCode == SocketError.OperationAborted) {
                break;
                
            } catch (Exception ex) {
                Console.WriteLine($"Exception: {ex.Message}");

            }
        }

        foreach (var c in clientRisuto)
            c.Close();

        Console.WriteLine("Server has been shutted down");
    }

    private static void isShuttingDown() {
        while (true) {
            string command = Console.ReadLine();

            if (command?.ToLower() == "exit") {
                isRunning = false;
                listener.Stop();
                Console.WriteLine("Server shutdown initiated");
                break;
            }
        }
    }

    private static async Task ProcessMessage(TcpClient client) {
        NetworkStream stream = client.GetStream();
        byte[] buffer = new byte[1024];

        while (true) {
            int numberBytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);

            if (numberBytesRead == 0) {
                clientRisuto.Remove(client);
                Console.WriteLine("Client disconnected");
                break;
            }

            string messageReceivedAndBroadcasting = Encoding.UTF8.GetString(buffer, 0, numberBytesRead);
            Console.WriteLine($"ReceivedAndWillBroadcast: {messageReceivedAndBroadcasting}");

            await BroadcastMessage(messageReceivedAndBroadcasting, client);
        }
    }

    private static async Task BroadcastMessage(string message, TcpClient sender) {
        byte[] buffer = Encoding.UTF8.GetBytes(message);

        foreach (var c in clientRisuto) {
            if (c == sender)
                continue;

            NetworkStream stream = c.GetStream();
            await stream.WriteAsync(buffer, 0, buffer.Length);
        }
    }
}