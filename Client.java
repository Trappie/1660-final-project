import java.net.*;
import java.io.*;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

public class Client{
  static JFrame frame;
  static JPanel mainPanel;
  static JPanel functionPanel;
  static JFileChooser fileChooser;

  static File[] inputFiles;
  static JLabel fileLabel;


  public static void main(String[] args) {
    frame = new JFrame();
    frame.setTitle("Hadoop");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    frame.setLayout(new FlowLayout());

    // mainPanel
    mainPanel = new JPanel();
    mainPanel.setLayout(new BoxLayout(mainPanel, BoxLayout.PAGE_AXIS));
    JButton chooseFilesButton = new JButton("Choose Files");
    fileLabel = new JLabel();

    // choose file view
    chooseFilesButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        // System.out.println("hello button");
        fileChooser = new JFileChooser(); // when click the button, use file chooser to choose file
        fileChooser.setMultiSelectionEnabled(true);
        int returnVal = fileChooser.showOpenDialog(frame);
        if (returnVal == JFileChooser.APPROVE_OPTION) {
          inputFiles = fileChooser.getSelectedFiles();
          String labelText = "<html>"; // use html for multi line label
          for (File f : inputFiles) {
            labelText += "<p>" + f.toString() + "</p>";
          }
          labelText += "</html>";
          fileLabel.setText(labelText);
        }
      }
    });
    JButton invertedIndexButton = new JButton("Construct Inverted Indicies");
    // TODO: when press invertedIndexButton, 1. upload files; 2. wait for hadoop to finish; 3. change panel visibility
    mainPanel.add(chooseFilesButton);
    mainPanel.add(fileLabel);
    mainPanel.add(invertedIndexButton);

    // function panel
    functionPanel = new JPanel();
    functionPanel.setLayout(new BoxLayout(functionPanel, BoxLayout.PAGE_AXIS));
    JButton termButton = new JButton("Search for Term");
    JButton topnButton = new JButton("Top-N");
    functionPanel.add(termButton);
    functionPanel.add(topnButton);





    frame.add(mainPanel);
    frame.add(functionPanel);
    frame.setSize(1000, 1000);
    frame.setVisible(true);

    // final JFileChooser fc = new JFileChooser();
    // int returnVal = fc.showOpenDialog(frame);

  }
}
