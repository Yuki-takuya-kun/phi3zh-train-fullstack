package phi3zh.service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import scala.concurrent.impl.FutureConvertersImpl;
import scala.xml.Elem;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * This class is aiming to create data for testing
 */
public class DataPreprocessor {
    /**
     * This function is going to transform the original text int wikipedia database xml format
     * @param sourceFolder the folder that contains the files of source wiki pages
     * @param targetFile the target file path that conserve the xml file
     * @throws Exception the exception
     */
    public static void wikiPreprocess(String sourceFolder, String targetFile) throws Exception{
        File sourceFolderPath = new File(sourceFolder);
        File[] sourceFiles = sourceFolderPath.listFiles();
        List<Pair<String, String>> titleText = Arrays.stream(sourceFiles).flatMap(file -> {
            String fileName = file.getName();
            int dotIdx = fileName.lastIndexOf('.');
            String title = fileName.substring(0, dotIdx);
            List<Pair<String, String>> res = new ArrayList<>();
            try {
                String text = new String(Files.readAllBytes(Paths.get(file.getPath())));
                res.add(Pair.of(title, text));
            } catch (Exception e){
               e.printStackTrace();
               System.out.println(1);
            }
            return res.stream();
        }).collect(Collectors.toList());

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = dbFactory.newDocumentBuilder();
        Document doc = documentBuilder.newDocument();

        Element root = doc.createElement("wikipedia");
        doc.appendChild(root);

        titleText.stream().forEach(elem -> {
            String title = elem.getLeft();
            String text = elem.getRight();
            Element pageXml = doc.createElement("page");
            Element titleXml = doc.createElement("title");
            titleXml.setTextContent(title);
            Element revisionXml = doc.createElement("revision");
            Element contentXml = doc.createElement("text");
            contentXml.setTextContent(text);
            contentXml.setAttribute("lang", "zh");
            revisionXml.appendChild(contentXml);
            pageXml.appendChild(titleXml);
            pageXml.appendChild(revisionXml);
            root.appendChild(pageXml);
        });

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(new File(targetFile));
        transformer.transform(source, result);
    }

}
