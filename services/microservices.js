import axios from "axios";
export async function sendData(excel) {
  const data = await axios.post("http://localhost:8000/upload", excel);
  return data;
}
