package co.psyke.utils.http

import scalaj.http.{HttpResponse, Http}

object APICalls {

    def apiGet(url:String) : String = {
        // URL dell'API
        try {
            // Effettua la richiesta GET all'API
            val response: HttpResponse[String] = Http(url).asString

            // Verifica se la richiesta ha avuto successo (codice di risposta 200)
            if (response.is2xx) {
                // Il corpo della risposta è disponibile come stringa JSON
                val jsonBody: String = response.body
                return jsonBody
            } else {
                println(s"Errore nella richiesta. Codice di risposta: ${response.code}")
            }
            "{}"
        } catch {
            case e: Exception =>
                // Gestisci eventuali eccezioni
                e.printStackTrace()
                "{}"
        }
    }
}
