package com.zr.hbase.util;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
/**
 * kerberos认证
 * @author zhairui
 *
 */

public class KerberosUtil {
	
	private final static AtomicBoolean INITIALIZED = new AtomicBoolean( false );
	
	public static void kerberosInit( final Configuration conf, String file ) throws IOException  {
		if( !(INITIALIZED.getAndSet( true ) ) ) {
			System.setProperty( "java.security.krb5.realm", "ZR.COM" );
			System.setProperty( "java.security.krb5.kdc", "d-bdap-yn-1.zr.com" );
			conf.set( "hadoop.security.authentication", "kerberos" );
			UserGroupInformation.setConfiguration( conf );
			UserGroupInformation.loginUserFromKeytab( "ZR@ZR.COM", file );
			
		}
	}
}
