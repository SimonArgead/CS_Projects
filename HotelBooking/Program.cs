using System;
using System.Collections.Generic;
using System.Linq;

class Room
{
    public int RoomNumber { get; set; }
    public string Type { get; set; }
    public string Beds { get; set; }
    public int PricePerNight { get; set; }
    public bool IsBooked { get; set; }
}

class Program
{
    static void Main()
    {
        List<Room> rooms = new List<Room>();
        int roomNumber = 1;

        // 10 rooms of each type, incl. price and bed configuration
        for (int i = 0; i < 10; i++)
            rooms.Add(new Room { RoomNumber = roomNumber++, Type = "1 person", Beds = "1 single bed", PricePerNight = 400 });

        for (int i = 0; i < 10; i++)
            rooms.Add(new Room { RoomNumber = roomNumber++, Type = "2 person", Beds = "1 doubble bed", PricePerNight = 800 });

        for (int i = 0; i < 10; i++)
            rooms.Add(new Room { RoomNumber = roomNumber++, Type = "3 person", Beds = "3 single beds eller 1 doubbel bed + 1 single bed", PricePerNight = 1200 });

        for (int i = 0; i < 10; i++)
            rooms.Add(new Room { RoomNumber = roomNumber++, Type = "4 person", Beds = "2 doubbel bed", PricePerNight = 1600 });

        while (true)
        {
            Console.Clear();
            Console.WriteLine("=== Hotel Booking System ===\n"); // welcome line

            // Show status for each type incl. room numbers
            var grouped = rooms.GroupBy(r => r.Type);
            foreach (var group in grouped)
            {
                int available = group.Count(r => !r.IsBooked);
                int booked = group.Count(r => r.IsBooked);
                int price = group.First().PricePerNight;
                int minNr = group.Min(r => r.RoomNumber);
                int maxNr = group.Max(r => r.RoomNumber);

                Console.WriteLine($"{group.Key} ({price} kr/night) | Rooms: {minNr}-{maxNr} | available: {available}, Booked: {booked}");
            }

            Console.WriteLine("\nType the room number you want to booke (or type 0 for cancel): ");
            if (!int.TryParse(Console.ReadLine(), out int selectNumber) || selectNumber < 0)
            {
                Console.WriteLine("Invalid input. Trype a keye to continue...");
                Console.ReadKey();
                continue;
            }

            if (selectNumber == 0) break;

            var selectRoom = rooms.FirstOrDefault(r => r.RoomNumber == selectNumber);

            if (selectRoom == null)
            {
                Console.WriteLine("The room doesn't exist.");
            }
            else if (selectRoom.IsBooked)
            {
                Console.WriteLine("The room is already booked.");
            }
            else
            {
                Console.Write("How many days do you want booke? ");
                if (int.TryParse(Console.ReadLine(), out int days) && days > 0)
                {
                    selectRoom.IsBooked = true;
                    int totalPrice = selectRoom.PricePerNight * days;

                    Console.WriteLine($"\n=== Booking confirmation ===");
                    Console.WriteLine($"Room nr. {selectRoom.RoomNumber} - {selectRoom.Type}");
                    Console.WriteLine($"Beds: {selectRoom.Beds}");
                    Console.WriteLine($"Price pr. night: {selectRoom.PricePerNight} kr");
                    Console.WriteLine($"Number of days: {days}");
                    Console.WriteLine($"Total price: {totalPrice} kr");
                }
                else
                {
                    Console.WriteLine("Invalid number of days.");
                }
            }

            Console.WriteLine("\nTrype a keye to continue...");
            Console.ReadKey();
        }
    }
}
