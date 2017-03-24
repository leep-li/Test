package com.boslog.fromexcel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;

public class ReadDataFromExcel {
	public List<List<List<String>>> readXls(String path) {
		// ReadDataFromExel excelReader = new ReadDataFromExel();
		File oldFile = null;
		InputStream input = null;
		HSSFWorkbook hssfWorkBook;
		List<List<List<String>>> result = new ArrayList<List<List<String>>>();
		try {
			oldFile = new File(path);// for rename
			if (oldFile.exists()) {
				input = new FileInputStream(path);
				hssfWorkBook = new HSSFWorkbook(input);// all excel
				for (int sheetNum = 0; sheetNum < hssfWorkBook.getNumberOfSheets(); sheetNum++) {
					List<List<String>> sheetList = new ArrayList<List<String>>();
					HSSFSheet hssfSheet = hssfWorkBook.getSheetAt(sheetNum);// one
																			// sheet
																			// of
																			// excel
					if (hssfSheet == null) {
						continue;
					}
					for (int rowNum = 0; rowNum <= hssfSheet.getLastRowNum(); rowNum++) {
						HSSFRow hssfRow = hssfSheet.getRow(rowNum);// one row of
																	// the
																	// sheet
						int minColIx = hssfRow.getFirstCellNum();
						int maxColIx = hssfRow.getLastCellNum();
						List<String> rowList = new ArrayList<String>();
						// every row
						for (int colIx = minColIx; colIx < maxColIx; colIx++) {
							HSSFCell cell = hssfRow.getCell(colIx);// one grid
							if (cell == null) {
								continue;
							}
							rowList.add(ReadDataFromExcel.getStringVal(cell));
						}
						sheetList.add(rowList);
					}
					result.add(sheetList);
				}
				SimpleDateFormat adf=new SimpleDateFormat("yyyy-MM-dd");
				Date da=new Date();
				String pathDate=adf.format(da);
				String rootPath = oldFile.getParent(); //get path of oldFile
				File newFile=new File(rootPath+File.separator+pathDate+"_"+"cob.xls");//new name 
				oldFile.renameTo(newFile);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	/*
	 * input = new FileInputStream(path); hssfWorkBook = new
	 * HSSFWorkbook(input);// all excel for (int sheetNum = 0; sheetNum <
	 * hssfWorkBook.getNumberOfSheets(); sheetNum++) { List<List<String>>
	 * sheetList = new ArrayList<List<String>>(); HSSFSheet hssfSheet =
	 * hssfWorkBook.getSheetAt(sheetNum);// one sheet // of excel if (hssfSheet
	 * == null) { continue; } for (int rowNum = 0; rowNum <=
	 * hssfSheet.getLastRowNum(); rowNum++) { HSSFRow hssfRow =
	 * hssfSheet.getRow(rowNum);// one row of the // sheet int minColIx =
	 * hssfRow.getFirstCellNum(); int maxColIx = hssfRow.getLastCellNum();
	 * List<String> rowList = new ArrayList<String>(); // every row for (int
	 * colIx = minColIx; colIx < maxColIx; colIx++) { HSSFCell cell =
	 * hssfRow.getCell(colIx);// one grid if (cell == null) { continue; }
	 * rowList.add(ReadDataFromExcel.getStringVal(cell)); }
	 * sheetList.add(rowList); } result.add(sheetList); } } catch (Exception e)
	 * { // TODO Auto-generated catch block e.printStackTrace(); }finally {
	 * if(input!=null){ try { input.close(); } catch (IOException e) { // TODO
	 * Auto-generated catch block e.printStackTrace(); } } }
	 */
	public static String getStringVal(HSSFCell cell) {
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_BOOLEAN:
			return cell.getBooleanCellValue() ? "TRUE" : "FALSE";
		case Cell.CELL_TYPE_FORMULA:
			return cell.getCellFormula();
		case Cell.CELL_TYPE_NUMERIC:
			cell.setCellType(Cell.CELL_TYPE_STRING);
			return cell.getStringCellValue();
		case Cell.CELL_TYPE_STRING:
			return cell.getStringCellValue();
		default:
			return " ";
		}
	}

}
