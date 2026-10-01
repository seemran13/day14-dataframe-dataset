package day14

/** Matches employees.csv exactly: column names and types must line up for .as[Employee] to work. */
final case class Employee(
  employeeId: String,
  name: String,
  department: String,
  baseSalary: Double,
  bonus: Double,
  deductions: Double,
  taxRate: Double
)

/** The typed result of the payroll calculation. */
final case class PayrollRecord(
  employeeId: String,
  name: String,
  department: String,
  grossPay: Double,
  netPay: Double
)
