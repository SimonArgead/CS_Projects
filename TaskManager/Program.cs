using System;
using System.Collections.Generic;
using System.Linq;

class TaskItem
{
    public int Id { get; set; }
    public string Description { get; set; }
    public string AssignedTo { get; set; }
    public DateTime DueDate { get; set; }
    public bool IsCompleted { get; set; }
}

class Program
{
    static void Main()
    {
        // 1. Setup initial data
        List<TaskItem> tasks = new List<TaskItem>();
        int nextId = 1;

        Console.WriteLine("=== Project Task Management System ===");
        Console.WriteLine("Press ENTER to start...");
        Console.ReadLine(); // User input to trigger the program

        // 2. Main menu loop
        while (true)
        {
            Console.WriteLine("\nChoose an action:");
            Console.WriteLine("1 - Add new task");
            Console.WriteLine("2 - Mark task as completed");
            Console.WriteLine("3 - View all tasks");
            Console.WriteLine("4 - View overdue tasks");
            Console.WriteLine("0 - Exit");
            Console.Write("Your choice: ");

            string choice = Console.ReadLine()?.Trim();

            if (choice == "0")
            {
                Console.WriteLine("Exiting system. Goodbye!");
                break;
            }
            else if (choice == "1")
            {
                // Add new task
                Console.Write("Enter task description: ");
                string desc = Console.ReadLine();

                Console.Write("Assign to (name): ");
                string assigned = Console.ReadLine();

                Console.Write("Enter due date (yyyy-mm-dd): ");
                if (DateTime.TryParse(Console.ReadLine(), out DateTime due))
                {
                    tasks.Add(new TaskItem
                    {
                        Id = nextId++,
                        Description = desc,
                        AssignedTo = assigned,
                        DueDate = due,
                        IsCompleted = false
                    });
                    Console.WriteLine("Task added successfully.");
                }
                else
                {
                    Console.WriteLine("Invalid date format.");
                }
            }
            else if (choice == "2")
            {
                // Mark task as completed
                Console.Write("Enter task ID to mark as completed: ");
                if (int.TryParse(Console.ReadLine(), out int id))
                {
                    var task = tasks.FirstOrDefault(t => t.Id == id);
                    if (task != null)
                    {
                        task.IsCompleted = true;
                        Console.WriteLine("Task marked as completed.");
                    }
                    else
                    {
                        Console.WriteLine("Task not found.");
                    }
                }
                else
                {
                    Console.WriteLine("Invalid ID.");
                }
            }
            else if (choice == "3")
            {
                // View all tasks
                if (tasks.Count == 0)
                {
                    Console.WriteLine("No tasks available.");
                }
                else
                {
                    foreach (var t in tasks)
                    {
                        Console.WriteLine($"[{t.Id}] {t.Description} | Assigned to: {t.AssignedTo} | Due: {t.DueDate:yyyy-MM-dd} | Completed: {t.IsCompleted}");
                    }
                }
            }
            else if (choice == "4")
            {
                // View overdue tasks
                var overdue = tasks.Where(t => !t.IsCompleted && t.DueDate < DateTime.Now).ToList();
                if (overdue.Count == 0)
                {
                    Console.WriteLine("No overdue tasks.");
                }
                else
                {
                    Console.WriteLine("Overdue tasks:");
                    foreach (var t in overdue)
                    {
                        Console.WriteLine($"[{t.Id}] {t.Description} | Assigned to: {t.AssignedTo} | Due: {t.DueDate:yyyy-MM-dd}");
                    }
                }
            }
            else
            {
                Console.WriteLine("Invalid choice.");
            }
        }
    }
}
