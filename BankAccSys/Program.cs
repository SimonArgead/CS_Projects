using System;
using System.Collections.Generic;

class Transaction
{
    public DateTime Timestamp { get; set; }
    public string Type { get; set; }
    public decimal Amount { get; set; }
    public decimal BalanceAfter { get; set; }
}

class Program
{
    static void Main()
    {
        // 1. Setup initial variables
        decimal balance = 10000m; // Starting balance in DKK
        List<Transaction> transactions = new List<Transaction>(); // Stores all transactions

        Console.WriteLine("=== Bank Account System ===");
        Console.WriteLine("Starting balance: " + balance + " DKK");

        // 2. Main program loop
        while (true)
        {
            // Get user input for action
            Console.WriteLine("\nChoose an action: (d) Deposit, (w) Withdraw, (q) Quit");
            Console.Write("Your choice: ");
            string choice = Console.ReadLine()?.Trim().ToLower();

            // Quit program
            if (choice == "q")
            {
                Console.WriteLine("\nExiting system. Goodbye!");
                break;
            }

            // Deposit money
            else if (choice == "d")
            {
                Console.Write("Enter deposit amount (DKK): ");
                if (decimal.TryParse(Console.ReadLine(), out decimal depositAmount) && depositAmount > 0)
                {
                    balance += depositAmount;

                    // Record transaction
                    transactions.Add(new Transaction
                    {
                        Timestamp = DateTime.Now,
                        Type = "Deposit",
                        Amount = depositAmount,
                        BalanceAfter = balance
                    });

                    Console.WriteLine($"Deposited {depositAmount} DKK. New balance: {balance} DKK");
                }
                else
                {
                    Console.WriteLine("Invalid amount.");
                }
            }

            // Withdraw money
            else if (choice == "w")
            {
                Console.Write("Enter withdrawal amount (DKK): ");
                if (decimal.TryParse(Console.ReadLine(), out decimal withdrawAmount) && withdrawAmount > 0)
                {
                    if (withdrawAmount <= balance)
                    {
                        balance -= withdrawAmount;

                        // Record transaction
                        transactions.Add(new Transaction
                        {
                            Timestamp = DateTime.Now,
                            Type = "Withdrawal",
                            Amount = withdrawAmount,
                            BalanceAfter = balance
                        });

                        Console.WriteLine($"Withdrew {withdrawAmount} DKK. New balance: {balance} DKK");
                    }
                    else
                    {
                        Console.WriteLine("Insufficient funds.");
                    }
                }
                else
                {
                    Console.WriteLine("Invalid amount.");
                }
            }

            else
            {
                Console.WriteLine("Invalid choice. Please enter 'd', 'w', or 'q'.");
            }

            // 3. Display transaction history
            Console.WriteLine("\n--- Transaction History ---");
            foreach (var t in transactions)
            {
                Console.WriteLine($"{t.Timestamp:G} | {t.Type} | {t.Amount} DKK | Balance after: {t.BalanceAfter} DKK");
            }
        }
    }
}
