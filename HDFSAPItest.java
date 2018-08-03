package mavend;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSAPItest {
    public static void main(String[] args) throws Exception {
        try {
            //check the args
            if ( args == null || args.length < 4 ) {
                //
                //invalid input parameters
                //无效输入参数
                throw new java.lang.Exception( "Invalid input parameters" );
            }
            else {
                //check
                if ( args[0].equals("-r") ) {
                    try {
                        //read
                        hdfsReadFile( args[1], args[2], args[3] );
                    }
                    catch ( java.lang.Exception e ) {
                        //write
                        System.out.println( e.getMessage() );
                    }
                }
                else if ( args[0].equals("-w") ) {
                    try {
                        //write
                        hdfsWriteFile( args[1], args[2], args[3] );
                    }
                    catch ( java.lang.Exception e ) {
                        //write
                        System.out.println( e.getMessage() );
                    }
                }
                else {
                    //输入有效参数
                    throw new java.lang.Exception( "Invalid input parameters" );
                }
            }
        }
        catch ( java.lang.Exception e ) {
            //usage
            //使用
            System.out.println("Usage:");
            System.out.println("java -jar hdfs.jar -r hdfsFile hdfsUrl localFile -- to read a file from HDFS and write as a local file");
            System.out.println("java -jar hdfs.jar -w localFile hdfsUrl hdfsFile -- to write a hdfs from a local file");
        }
    }

    /*
     * Read a file from HDFS, and write as specified local file
     * 从HDFS读取文件，并写入指定的本地文件
     */
    private static void hdfsReadFile( String hdfsFile, String hdfsUrl, String fileName ) throws java.lang.Exception {
        //hdfs configuration HDFS配置
        Configuration cfg = new Configuration();
        cfg.set("fs.defaultFS", hdfsUrl);
        //hdfs
        FileSystem fsSource = FileSystem.get(cfg);
        //check if the target file already exists
        //检查目标文件是否已经存在
        if ( !fsSource.exists(new Path(hdfsFile)) ) {
            //error out
            throw new java.lang.Exception("The source HDFS file doesn't exists!");
        }

        try {
            //open
            FSDataInputStream fsdiStream = fsSource.open( new Path(hdfsFile) );
            try {
                //local file
                //本地文件
                java.io.FileOutputStream foStream = new java.io.FileOutputStream( fileName );
                try {
                    //buffer 缓冲区
                    byte[] buffer = new byte[2048];
                    //first read  首读
                    int count = fsdiStream.read(buffer, 0, 2048);
                    //loop for write and further read  写和再读循环
                    while ( count > 0 ) {
                        //write
                        foStream.write( buffer, 0, count );
                        //further read  进一步阅读
                        count = fsdiStream.read(buffer, 0, 2048);
                    }
                }
                finally {
                    //close
                    foStream.close();
                }
            }
            finally {
                //close
                fsdiStream.close();
            }
        }
        finally {
            //close
            fsSource.close();
        }
    }

    /*
     * Write a local file into HDFS
     * 在HDFS中写入本地文件
     */
    private static void hdfsWriteFile( String fileName, String hdfsUrl, String hdfsFile ) throws java.lang.Exception {
        //hdfs configuration  HDFS配置
        Configuration cfg = new Configuration();
        cfg.set("fs.defaultFS", hdfsUrl);
        //hdfs
        FileSystem fsTarget = FileSystem.get(cfg);
        //check if the target file already exists    检查目标文件是否已经存在
        if ( fsTarget.exists(new Path(hdfsFile)) ) {
            //error out
            throw new java.lang.Exception("The target HDFS file already exists!");
        }

        try {
            //open
            FSDataOutputStream fsdiStream = fsTarget.create( new Path(hdfsFile) );
            try {
                //local file
                java.io.FileInputStream fiStream = new java.io.FileInputStream( fileName );
                try {
                    //buffer
                    byte[] buffer = new byte[2048];
                    //first read
                    int count = fiStream.read(buffer, 0, 2048);
                    //loop for write and further read  写和再读循环
                    while ( count > 0 ) {
                        //write
                        fsdiStream.write( buffer, 0, count );
                        //further read  进一步阅读
                        count = fiStream.read(buffer, 0, 2048);
                    }
                }
                finally {
                    //close
                    fiStream.close();
                }
            }
            finally {
                //close
                fsdiStream.close();
            }
        }
        finally {
            //close
            fsTarget.close();
        }
    }
}



