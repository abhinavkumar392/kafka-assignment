package com.example.kafkaassignment.service;

import com.example.kafkaassignment.constants.AppConstants;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class TextToJson {

    @Autowired
    private final KafkaProducer producer;
    @Autowired
    private final StreamService stream;
    public TextToJson(KafkaProducer producer, StreamService stream) {
        this.producer = producer;
        this.stream = stream;
    }

    public Map<Integer, HealthDataMapping> subscriberMappings;
    public Map<Integer, HealthDataMapping> patientMappings;
    public Map<Integer, HealthDataMapping> caseMappings;
    public Map<Integer, HealthDataMapping> serviceMappings;

    public record HealthDataMapping(String fieldName, int start, int end, String outputFieldName) { }

    public void transformTxtToJson(){
        String textFilePath = AppConstants.txtFilePath;
        String excelFilePath = AppConstants.xlsxFilePath;


        try {
            subscriberMappings = readDataMappings(excelFilePath, "Subscriber");
            patientMappings = readDataMappings(excelFilePath, "Patient");
            caseMappings = readDataMappings(excelFilePath, "Case");
            serviceMappings = readDataMappings(excelFilePath, "Service");


            parseHealthTextFile(textFilePath);
            stream.kstream();
            System.out.println("Data Transformed Successfully");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static Map<Integer, HealthDataMapping> readDataMappings(String excelFilePath, String sheetName) throws IOException {
        Map<Integer, HealthDataMapping> dataMappings = new HashMap<>();

        try (Workbook workbook = new XSSFWorkbook(new FileInputStream(excelFilePath))) {
            Sheet sheet = workbook.getSheet(sheetName);
            for (Row row : sheet) {
                if (row.getCell(0).getCellType() == CellType.STRING && row.getCell(0).getStringCellValue().equals("Seq No")) {
                    continue;
                }

                int seqNo = (int) row.getCell(0).getNumericCellValue();
                if (sheetName.equals("Case") && seqNo == 0) {
                    break;
                }
                String fieldName = row.getCell(1).getStringCellValue();
                int start = (int) row.getCell(2).getNumericCellValue();
                int end = (int) row.getCell(3).getNumericCellValue();
                String outputFieldName = row.getCell(4).getStringCellValue();
                dataMappings.put(seqNo, new HealthDataMapping(fieldName, start, end, outputFieldName));
            }
        }

        return dataMappings;
    }

    private Map<Integer, HealthDataMapping> getDataMappingsForRecordType(String recordType) {
        return switch (recordType) {
            case "SUB" -> subscriberMappings;
            case "PAT" -> patientMappings;
            case "CAS" -> caseMappings;
            case "SVC" -> serviceMappings;
            default -> throw new IllegalArgumentException("Invalid record type: " + recordType);
        };
    }
    private void parseHealthTextFile(String textFilePath) throws IOException {

        try (BufferedReader reader = new BufferedReader(new FileReader(textFilePath))) {
            int subCount = 0;
            int svcCount = 0;
            String line;
            String currentRecordType = "";
            Map<String, String> currentRecord = null;
            Map<String, Map<String, String>> tempOutput = new HashMap<>();

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("SUB") || line.startsWith("PAT") || line.startsWith("CAS") || line.startsWith("SVC")) {
                    currentRecordType = line.substring(0, 3);
                    currentRecord = new HashMap<>();
                    tempOutput.put(currentRecordType, currentRecord);
                }
                if ("SUB".equals(currentRecordType)) {
                    subCount++;
                }
                if ("SVC".equals(currentRecordType)) {
                    svcCount++;
                }
                if (currentRecord != null) {
                    if ("TRL20220915".equals(line)) {
                        continue;
                    }
                    Map<Integer, HealthDataMapping> mapping = getDataMappingsForRecordType(currentRecordType);

                    for (Map.Entry<Integer, HealthDataMapping> entry : mapping.entrySet()) {
                        int start = entry.getValue().start();
                        int end = entry.getValue().end();
                        String fieldName = entry.getValue().fieldName();
                        String outputFieldName = entry.getValue().outputFieldName();

                        if (line.length() >= end) {
                            String value = line.substring(start - 1, end).trim();
                            currentRecord.put(outputFieldName, value);
                        }
                    }
                    if (subCount == svcCount) {
                        producer.sendMessage(tempOutput);
                        tempOutput = new HashMap<>();
                    }
                }
            }
        }
    }

}

