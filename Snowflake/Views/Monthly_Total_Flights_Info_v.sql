Create or Replace View Monthly_Total_Flights_Info_v As
Select f.Airline,
       Ar.Airline as Airline_Name,
       f.Origin_Airport As Airport,
       Ap.Airport as Airport_Name,
       Ap.city||' - '||Ap.State||', '||Ap.Country as Airport_Location,
       f.Month,
       Count(f.Flight_Number) As Total_Flights
From Flights f,
     Airlines ar,
     Airports Ap
Where f.Airline = ar.iata_code
And   f.Origin_Airport = Ap.iata_code
Group By f.Airline,
         Ar.Airline,
         f.Origin_Airport,
         Ap.Airport,
         Ap.city||' - '||Ap.State||', '||Ap.Country,
         f.Month
Order By f.Airline,
         f.Month,
         f.Origin_Airport;

Grant Select on Monthly_Total_Flights_Info_v to Public;