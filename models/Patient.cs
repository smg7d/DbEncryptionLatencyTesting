
namespace LatencyTesting
{
    public class Patient
    {
        public long Id { get; set; }
        public Guid PatientId { get; set; }
        public string? Name { get; set; }
        public string? Email {get; set;}
        public string? PhoneNumber { get; set;}
        public DateOnly DateOfBirth { get; set;}
        public List<Observation> Observations { get; set; } = new List<Observation>();
    }

    public class Observation
    {
        public long Id { get; set;}
        public Guid PatientId {get; set;}
        public string? Name {get; set;}
        public int? Height { get; set; }
        public int? Weight { get; set; }
        public string? BloodPressure { get; set; }
        public DateTime ObservationDate {get; set;}
    }
}