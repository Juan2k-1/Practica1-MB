/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package juan.practica1_mb;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 *
 * @author juald
 */
public class ParserDocument {

    public static void main(String[] args) throws IOException, SolrServerException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Por favor, proporciona la ruta del archivo CISI.ALL como argumento.");
        String filePath = scanner.nextLine();

        Path pathToDocument = Paths.get(filePath);

        final SolrClient solr = new HttpSolrClient.Builder("http://localhost:8983/solr").build();
        BufferedReader br = Files.newBufferedReader(pathToDocument.toAbsolutePath());

        String line;
        StringBuilder documentContent = new StringBuilder();
        boolean inDocument = false;
        String id = null;
        String title = null;
        String author = null;
        StringBuilder content = new StringBuilder();

        while ((line = br.readLine()) != null) {
            if (line.startsWith(".I")) {
                // Nuevo documento comienza
                if (inDocument) {
                    // Si ya estábamos en un documento, enviamos el documento a Solr
                    SolrInputDocument document = new SolrInputDocument();
                    document.addField("id", id);
                    document.addField("title", title);
                    document.addField("author", author);
                    document.addField("content", content.toString());
                    solr.add("CORPUS", document);
                    documentContent.setLength(0); // Limpiamos el contenido del documento
                }
                inDocument = true;
                id = line.substring(3).trim(); // Obtener el ID del documento
                content.setLength(0); // Limpiar el contenido
            } else if (line.startsWith(".T")) {
                // Título del documento
                title = br.readLine().trim();
            } else if (line.startsWith(".A")) {
                // Autor del documento
                author = br.readLine().trim();
            } else if (line.startsWith(".W")) {
                // Contenido del documento
                content.append(br.readLine().trim()).append(" ");
            }
        }

        // Procesamos el último documento 
        if (inDocument) {
            SolrInputDocument document = new SolrInputDocument();
            document.addField("id", id);
            document.addField("title", title);
            document.addField("author", author);
            document.addField("content", content.toString());
            solr.add("CORPUS", document);
        }

        // Enviar los cambios al servidor Solr
        solr.commit("CORPUS");
        solr.close();
        br.close();
    }
}
