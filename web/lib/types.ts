export interface Flight {
  flight_id: string;
  carrier: string;
  flight_number: number;
  tail_number: string | null;
  origin: string;
  dest: string;
  sched_dep_utc: string;
  sched_arr_utc: string;
  cancelled: boolean;
}

export interface RiskAssessment {
  ts: string;
  model_version: string;
  p_miss: number | null;
  predicted_delay: number;
  explanation: string[];
}

export interface FlightRisk {
  flight_id: string;
  model_name: string;
  model_version: string;
  ts: number;
  source_event_type: string;
  predicted_delay: number;
  p_miss: number | null;
  explanation: string[];
}

export interface FlightDetail {
  flight: Flight;
  chain: Flight[];
  risk: FlightRisk | null;
}

export interface TripLeg {
  id: string;
  flight_id: string;
  leg_order: number;
  min_connection_min: number | null;
  flight: Flight;
  risk: RiskAssessment | null;
}

export interface Trip {
  id: string;
  user_id: string;
  status: string;
  to_email: string | null;
  push_subscription: unknown;
  created_at: string;
  legs: TripLeg[];
}

export interface TripSummary {
  id: string;
  status: string;
  created_at: string;
}
